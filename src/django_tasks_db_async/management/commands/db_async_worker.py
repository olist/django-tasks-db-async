"""Django management command that runs async task workers backed by django-tasks-db."""

import asyncio
import gc
import itertools
import logging
import random
import signal
import threading
from argparse import ArgumentParser
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, cast

from asgiref.sync import ThreadSensitiveContext, sync_to_async
from django.core.exceptions import SuspiciousOperation
from django.core.management.base import BaseCommand
from django.db import close_old_connections, transaction
from django.utils.crypto import get_random_string
from django_tasks_db.compat import (
    DEFAULT_TASK_BACKEND_ALIAS,
    DEFAULT_TASK_QUEUE_NAME,
    TaskContext,
    task_finished,
    task_started,
)
from django_tasks_db.models import DBTaskResult
from opentelemetry import trace
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_CLIENT_ID,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
    MessagingOperationTypeValues,
)

from django_tasks_db_async._compat import TaskGroup

if TYPE_CHECKING:
    from django_tasks_db.compat import BaseTaskResult

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class Worker:
    """Async worker that polls queues, claims tasks atomically, and executes them."""

    def __init__(self, worker_id: str, backend_name: str, interval: float) -> None:
        """Initialize the worker with a unique ID, target backend, and polling interval."""
        self.worker_id = worker_id
        self.backend_name = backend_name
        self.interval = interval
        self._tasks_run = 0

    @sync_to_async
    def _claim_task(self, queue_names: Sequence[str]) -> DBTaskResult[Any, Any] | None:
        with transaction.atomic():
            tasks = DBTaskResult.objects.ready().filter(backend_name=self.backend_name)
            if queue_names != ["*"]:
                tasks = tasks.filter(queue_name__in=queue_names)

            task = tasks.select_for_update(skip_locked=True, no_key=True).first()
            if task is not None:
                task.claim(self.worker_id)
            return task

    async def run(
        self,
        queue_names: Sequence[str],
        *,
        batch: bool,
        stop_sign: asyncio.Event,
        task_counter: Iterator[None],
    ) -> None:
        """Poll queues in a loop until stop_sign is set, dispatching each claimed task."""
        logger.info(
            "Worker started worker_id=%r queue_names=%r backend=%r interval=%r batch=%r",
            self.worker_id,
            queue_names,
            self.backend_name,
            self.interval,
            batch,
        )

        await asyncio.sleep(random.random())  # noqa: S311

        while not stop_sign.is_set():
            async with ThreadSensitiveContext(), TaskGroup() as tg:  # type: ignore[no-untyped-call]
                try:
                    task = await self._claim_task(queue_names)
                    if task:
                        await tg.create_task(self.run_task(task))
                        next(task_counter)
                finally:
                    await sync_to_async(close_old_connections)()

                if not task:
                    if batch:
                        break

                    await asyncio.sleep(self.interval)

        logger.info("Worker stopped worker_id=%r tasks_run=%r", self.worker_id, self._tasks_run)

    async def run_task(self, db_task_result: DBTaskResult[Any, Any]) -> None:
        """Execute the task, fire task signals, and record success or failure on the DBTaskResult."""
        with tracer.start_as_current_span(
            f"{db_task_result.queue_name} process",
            kind=trace.SpanKind.CONSUMER,
            attributes={
                MESSAGING_SYSTEM: "django_tasks_db",
                MESSAGING_OPERATION_TYPE: MessagingOperationTypeValues.PROCESS.value,
                MESSAGING_DESTINATION_NAME: db_task_result.queue_name,
                MESSAGING_MESSAGE_ID: str(db_task_result.id),
                MESSAGING_CLIENT_ID: self.worker_id,
            },
        ) as span:
            task_func_name: str | None = None
            try:
                task = db_task_result.task
                task_result = cast("BaseTaskResult[Any, Any]", db_task_result.task_result)
                backend_type = type(task.get_backend())
                task_func_name = f"{task.func.__module__}.{task.func.__qualname__}"
                span.set_attribute(MESSAGING_OPERATION_NAME, task_func_name)

                logger.info(
                    "Running task worker_id=%r task_id=%r queue=%r task=%r",
                    self.worker_id,
                    db_task_result.id,
                    db_task_result.queue_name,
                    task_func_name,
                )
                await task_started.asend(backend_type, task_result=task_result)
                if task.takes_context:
                    return_value = await task.acall(
                        TaskContext(task_result=task_result),
                        *task_result.args,
                        **task_result.kwargs,
                    )
                else:
                    return_value = await task.acall(*task_result.args, **task_result.kwargs)

                # Setting the return and success value inside the error handling,
                # So errors setting it (eg JSON encode) can still be recorded
                await sync_to_async(db_task_result.set_successful)(return_value)
                await task_finished.asend(backend_type, task_result=db_task_result.task_result)
                duration = (db_task_result.finished_at - db_task_result.started_at).total_seconds()
                logger.info(
                    "Task complete worker_id=%r task_id=%r queue=%r task=%r duration=%r",
                    self.worker_id,
                    db_task_result.id,
                    db_task_result.queue_name,
                    task_func_name,
                    duration,
                )
            except BaseException as e:
                span.record_exception(e)
                span.set_status(trace.StatusCode.ERROR)
                await sync_to_async(db_task_result.set_failed)(e)
                duration = (db_task_result.finished_at - db_task_result.started_at).total_seconds()
                logger.exception(
                    "Task failed worker_id=%r task_id=%r queue=%r task=%r duration=%r",
                    self.worker_id,
                    db_task_result.id,
                    db_task_result.queue_name,
                    task_func_name,
                    duration,
                )

                try:
                    task_result = cast("BaseTaskResult[Any, Any]", db_task_result.task_result)
                except (ImportError, SuspiciousOperation):
                    pass
                else:
                    await task_finished.asend(backend_type, task_result=task_result)
            finally:
                self._tasks_run += 1


class Command(BaseCommand):
    """Management command that parses arguments and runs the async worker pool."""

    help = ""

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Register CLI arguments for queue, interval, concurrency, and worker identity."""
        parser.add_argument(
            "--queue-name",
            nargs="?",
            default=DEFAULT_TASK_QUEUE_NAME,
            type=str,
            help="The queues to process. Separate multiple with a comma. To process all queues, use '*' (default: %(default)r)",  # noqa: E501
        )
        parser.add_argument(
            "--interval",
            nargs="?",
            default=1,
            type=float,
            help="The interval (in seconds) to wait, when there are no tasks in the queue, before checking for tasks again (default: %(default)r)",  # noqa: E501
        )
        parser.add_argument(
            "--batch",
            action="store_true",
            help="Process all outstanding tasks, then exit. Can be used in combination with --max-tasks.",
        )
        parser.add_argument(
            "--backend",
            nargs="?",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            type=str,
            dest="backend_name",
            help="The backend to operate on (default: %(default)r)",
        )
        parser.add_argument(
            "--max-tasks",
            nargs="?",
            default=None,
            type=int,
            help="If provided, the maximum number of tasks each worker will execute before exiting.",
        )
        parser.add_argument(
            "--worker-id",
            nargs="?",
            type=str,
            help="Worker id. MUST be unique across worker pool (default: auto-generate)",
            default=get_random_string(32),
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=10,
            help="Maximum number of concurrent operations (default: %(default)r)",
        )

    def handle(  # noqa: PLR0913
        self,
        *,
        queue_name: str,
        interval: float,
        batch: bool,
        backend_name: str,
        max_tasks: int | None,
        worker_id: str,
        concurrency: int,
        **_options: Any,  # noqa: ANN401
    ) -> None:
        """Freeze GC, create an event loop with eager task factory, and delegate to ahandle."""
        gc.collect()
        gc.freeze()

        with asyncio.Runner() as runner:
            runner.get_loop().set_task_factory(asyncio.eager_task_factory)
            runner.run(
                self.ahandle(
                    queue_name=queue_name,
                    interval=interval,
                    batch=batch,
                    backend_name=backend_name,
                    max_tasks=max_tasks,
                    worker_id=worker_id,
                    concurrency=concurrency,
                ),
            )

    async def ahandle(  # noqa: PLR0913
        self,
        *,
        queue_name: str,
        interval: float,
        batch: bool,
        backend_name: str,
        max_tasks: int | None,
        worker_id: str,
        concurrency: int,
    ) -> None:
        """Spawn concurrency Worker coroutines and wire SIGTERM/SIGINT for graceful shutdown."""
        queue_names = queue_name.split(",")
        stop_sign = asyncio.Event()

        logger.info(
            "Starting db_async_worker worker_id=%r backend_name=%r queue_names=%r"
            " concurrency=%r interval=%r batch=%r max_tasks=%r",
            worker_id,
            backend_name,
            queue_names,
            concurrency,
            interval,
            batch,
            max_tasks,
        )

        def request_shutdown(signum: int) -> None:
            logger.info(
                "Shutdown requested, waiting for running tasks to finish worker_id=%r backend_name=%r signal=%r",
                worker_id,
                backend_name,
                signal.Signals(signum).name,
            )
            stop_sign.set()

        if threading.current_thread() is threading.main_thread():
            loop = asyncio.get_running_loop()
            for signum in [signal.SIGTERM, signal.SIGINT]:
                loop.add_signal_handler(signum, request_shutdown, signum)

        def total_tasks() -> Iterator[None]:
            for i in itertools.count(1):
                if max_tasks is not None and i >= max_tasks and not stop_sign.is_set():
                    logger.info(
                        "Max tasks reached, stopping worker_id=%r backend_name=%r max_tasks=%r",
                        worker_id,
                        backend_name,
                        max_tasks,
                    )
                    stop_sign.set()
                yield

        async with TaskGroup() as tg:
            for i in range(concurrency):
                tg.create_task(
                    Worker(
                        f"{worker_id}-{i:02}",
                        backend_name,
                        interval,
                    ).run(
                        queue_names,
                        batch=batch,
                        stop_sign=stop_sign,
                        task_counter=total_tasks(),
                    ),
                )
