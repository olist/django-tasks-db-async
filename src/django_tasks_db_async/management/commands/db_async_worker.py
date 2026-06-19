"""Django management command that runs async task workers backed by django-tasks-db."""

import asyncio
import gc
import logging
import signal
import threading
from argparse import ArgumentParser
from typing import TYPE_CHECKING, Any, cast

from asgiref.sync import ThreadSensitiveContext, sync_to_async
from django.core.exceptions import SuspiciousOperation
from django.core.management.base import BaseCommand
from django.db import close_old_connections, transaction
from django.utils.crypto import get_random_string
from django_tasks_db.models import DBTaskResult, TaskResultStatus
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

from django_tasks_db_async._compat import (
    DEFAULT_TASK_BACKEND_ALIAS,
    DEFAULT_TASK_QUEUE_NAME,
    BaseExceptionGroup,
    TaskContext,
    TaskGroup,
    dispatch_signal,
    task_finished,
    task_started,
)

if TYPE_CHECKING:
    from django_tasks_db_async._compat import TaskResult

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class TerminateTaskGroup(BaseException):
    """Raised inside a TaskGroup to cancel all sibling tasks and exit the group immediately."""


class Worker:
    """Async worker that polls queues, claims tasks atomically, and executes them."""

    def __init__(self, worker_id: str, backend_name: str) -> None:
        """Initialize the worker with a unique ID, target backend, and polling interval."""
        self.worker_id = worker_id
        self.backend_name = backend_name
        self._tasks_run = 0
        self._stop_sign = asyncio.Event()

    async def run(self, work_queue: asyncio.Queue[DBTaskResult]) -> None:
        """Poll queues in a loop until stop_sign is set, dispatching each claimed task."""
        logger.info("Worker started, worker_id=%r", self.worker_id)

        async with ThreadSensitiveContext(), TaskGroup() as tg:  # type: ignore[no-untyped-call]
            stop_task = tg.create_task(self._stop_sign.wait())
            while True:
                work_task = tg.create_task(work_queue.get())
                await asyncio.wait([work_task, stop_task], return_when=asyncio.FIRST_COMPLETED)

                if work_task.done():
                    try:
                        await tg.create_task(self.run_task(work_task.result()))
                        work_queue.task_done()
                    finally:
                        await sync_to_async(close_old_connections)()
                else:
                    work_task.cancel()

                if self._stop_sign.is_set():
                    break

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
                task_result = cast("TaskResult[Any, Any]", db_task_result.task_result)
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
                await dispatch_signal(task_started, backend_type, task_result=task_result)
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
                await dispatch_signal(task_finished, backend_type, task_result=db_task_result.task_result)
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
                    task_result = cast("TaskResult[Any, Any]", db_task_result.task_result)
                except (ImportError, SuspiciousOperation):
                    pass
                else:
                    await dispatch_signal(task_finished, backend_type, task_result=task_result)
            finally:
                self._tasks_run += 1

    def stop(self) -> None:
        """Signal the worker to stop accepting new tasks after the current one finishes."""
        self._stop_sign.set()


class Supervisor:
    """Orchestrates a pool of Workers: claims tasks from the DB, feeds them via a queue, and handles shutdown."""

    def __init__(self, backend_name: str, queue_names: list[str], worker_id: str, max_tasks: int | None = None) -> None:
        """Initialize the supervisor with target backend, queue names, worker identity, and optional task cap."""
        self.backend_name = backend_name
        self.queue_names = queue_names
        self.worker_id = worker_id
        self.max_tasks = max_tasks
        self.shutdown_requested = False
        self.force_shutdown_sign = asyncio.Event()
        self.total_tasks = 0
        self.spawned_workers = 0
        self.workers: set[Worker] = set()

    async def run(
        self,
        *,
        concurrency: int,
        interval: float | None = None,
        batch: bool,
    ) -> None:
        """Start the worker pool, pull tasks from the DB, and drain the queue on shutdown.

        In batch mode exits after all currently-ready tasks are processed; otherwise
        polls continuously at the given interval until a shutdown is requested.
        """
        queue = asyncio.Queue[DBTaskResult](concurrency)

        async def _force_shutdown() -> None:
            await self.force_shutdown_sign.wait()
            raise TerminateTaskGroup

        try:
            async with TaskGroup() as tg:
                forced_shutdown_task = tg.create_task(_force_shutdown())
                puller = tg.create_task(
                    self._pull_tasks(
                        queue,
                        interval=interval or 0,
                        concurrency=concurrency,
                        batch=batch,
                    )
                )

                for _ in range(concurrency):
                    self._spawn_worker(tg, queue)

                await puller
                tg.create_task(self._unclaim(queue))
                await queue.join()
                self._shutdown_workers()
                forced_shutdown_task.cancel()

        except BaseExceptionGroup as exc:
            _, remaining = exc.split(TerminateTaskGroup)
            if remaining:
                raise remaining from exc

    def request_shutdown(self) -> None:
        """Request a graceful shutdown: stop claiming new tasks and let running tasks finish."""
        self.shutdown_requested = True
        self._shutdown_workers()

    def force_shutdown(self) -> None:
        """Trigger an immediate shutdown by raising TerminateTaskGroup, abandoning in-flight tasks."""
        self.force_shutdown_sign.set()

    def _spawn_worker(self, tg: TaskGroup, work_queue: asyncio.Queue[DBTaskResult]) -> Worker:
        worker = Worker(f"{self.worker_id}-{self.spawned_workers:02}", backend_name=self.backend_name)
        task = tg.create_task(worker.run(work_queue))
        task.add_done_callback(lambda _: self.workers.discard(worker))
        self.workers.add(worker)
        self.spawned_workers += 1
        return worker

    def _shutdown_workers(self) -> None:
        for w in self.workers:
            w.stop()

    @sync_to_async
    def _claim_tasks(self, amount: int) -> list[DBTaskResult]:
        with transaction.atomic():
            tasks_qs = DBTaskResult._default_manager.ready().filter(backend_name=self.backend_name)
            if self.queue_names != ["*"]:
                tasks_qs = tasks_qs.filter(queue_name__in=self.queue_names)

            tasks = list(tasks_qs[:amount].select_for_update(skip_locked=True, no_key=True))
            for task in tasks:
                task.claim(self.worker_id)

            close_old_connections()
            return tasks

    async def _pull_tasks(
        self,
        queue: asyncio.Queue[DBTaskResult],
        *,
        interval: float,
        concurrency: int,
        batch: bool,
    ) -> None:
        async with ThreadSensitiveContext():
            while not self.shutdown_requested:
                if queue.qsize() >= len(self.workers):
                    await asyncio.sleep(interval)
                    continue

                tasks_to_claim = concurrency
                if self.max_tasks is not None:
                    remaining_tasks = max(self.max_tasks - self.total_tasks, 0)

                    if remaining_tasks == 0:
                        break

                    tasks_to_claim = min(remaining_tasks, concurrency)

                tasks = await self._claim_tasks(tasks_to_claim)
                for task in tasks:
                    await queue.put(task)

                self.total_tasks += len(tasks)

                if not tasks:
                    if batch:
                        break

                    await asyncio.sleep(interval)

    async def _unclaim(self, queue: asyncio.Queue[DBTaskResult]) -> None:
        async with ThreadSensitiveContext():
            # Unclaim tasks
            while not queue.empty():
                task = await queue.get()
                task.status = TaskResultStatus.READY
                task.started_at = None
                task.worker_ids = task.worker_ids[:-1]
                await task.asave()
                queue.task_done()

            await sync_to_async(close_old_connections)()


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

        supervisor = Supervisor(backend_name, queue_names, worker_id, max_tasks)

        def request_shutdown(signum: int) -> None:
            if not supervisor.shutdown_requested:
                logger.info(
                    "Shutdown requested, waiting for running tasks to finish worker_id=%r backend_name=%r signal=%r",
                    worker_id,
                    backend_name,
                    signal.Signals(signum).name,
                )
                supervisor.request_shutdown()
            elif signum == signal.SIGINT:
                logger.info(
                    "Forcing shutdown of worker_id=%r backend_name=%r signal=%r",
                    worker_id,
                    backend_name,
                    signal.Signals(signum).name,
                )
                supervisor.force_shutdown()

        if threading.current_thread() is threading.main_thread():
            loop = asyncio.get_running_loop()
            for signum in [signal.SIGTERM, signal.SIGINT]:
                loop.add_signal_handler(signum, request_shutdown, signum)

        await supervisor.run(concurrency=concurrency, interval=interval, batch=batch)
