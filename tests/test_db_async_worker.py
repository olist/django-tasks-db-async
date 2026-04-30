import asyncio
import itertools
from collections.abc import Generator, Iterator
from typing import Any
from unittest import mock

import pytest
from asgiref.sync import async_to_sync
from django.test import override_settings
from django_tasks_db.models import DBTaskResult
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from django_tasks_db_async._compat import TaskResultStatus, task, task_finished
from django_tasks_db_async.management.commands.db_async_worker import Command, Worker

pytestmark = pytest.mark.django_db


@task()
def _noop_task() -> str:
    return "ok"


@task()
async def _failing_task() -> None:
    msg = "task error"
    raise ValueError(msg)


class TestWorker:
    @pytest.fixture(autouse=True)
    def _tasks_settings(self) -> Generator[None, Any]:
        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_tasks_db.DatabaseBackend",
                    "QUEUES": ["default", "other"],
                },
            },
        ):
            yield

    @pytest.fixture(autouse=True)
    def mock_close_old_connections(self) -> Generator[mock.AsyncMock, Any]:
        with mock.patch(
            "django_tasks_db_async.management.commands.db_async_worker.close_old_connections",
        ) as mock_close:
            yield mock_close

    def test_run_batch_mode_exits_immediately_when_no_tasks_available(
        self,
        mock_close_old_connections: mock.Mock,
    ) -> None:
        """Test if run in batch mode exits without processing when the queue is empty."""
        worker = Worker("w-00", "default", interval=0)
        with mock.patch.object(worker, "run_task") as mock_run_task:
            async_to_sync(worker.run)(
                ["*"],
                batch=True,
                stop_sign=asyncio.Event(),
                task_counter=itertools.repeat(None),
            )
        mock_run_task.assert_not_awaited()
        mock_close_old_connections.assert_called_once()

    def test_run_wildcard_queue_processes_tasks_from_any_queue(
        self,
        mock_close_old_connections: mock.Mock,
    ) -> None:
        """Test if run with queue_names=['*'] processes tasks regardless of their queue."""
        enqueued_tasks = [_noop_task.using(queue_name="other").enqueue()]
        worker = Worker("w-00", "default", interval=0)
        with mock.patch.object(worker, "run_task") as mock_run_task:
            async_to_sync(worker.run)(
                ["*"],
                batch=True,
                stop_sign=asyncio.Event(),
                task_counter=itertools.repeat(None),
            )
        assert mock_run_task.await_count == len(enqueued_tasks)
        assert mock_close_old_connections.call_count == len(enqueued_tasks) + 1

    def test_run_specific_queue_skips_tasks_in_other_queues(self, mock_close_old_connections: mock.Mock) -> None:
        """Test if run with a specific queue name does not process tasks in other queues."""
        _noop_task.using(queue_name="other").enqueue()
        worker = Worker("w-00", "default", interval=0)
        with mock.patch.object(worker, "run_task") as mock_run_task:
            async_to_sync(worker.run)(
                ["my-queue"],
                batch=True,
                stop_sign=asyncio.Event(),
                task_counter=itertools.repeat(None),
            )
        mock_run_task.assert_not_awaited()
        mock_close_old_connections.assert_called_once()

    def test_run_batch_mode_processes_all_available_tasks_then_exits(
        self,
        mock_close_old_connections: mock.Mock,
    ) -> None:
        """Test if run in batch mode processes all available tasks before exiting."""
        enqueued_tasks = [_noop_task.enqueue(), _noop_task.enqueue()]
        worker = Worker("w-00", "default", interval=0)
        with mock.patch.object(worker, "run_task") as mock_run_task:
            async_to_sync(worker.run)(
                ["*"],
                batch=True,
                stop_sign=asyncio.Event(),
                task_counter=itertools.repeat(None),
            )
        assert mock_run_task.await_count == len(enqueued_tasks)
        assert mock_close_old_connections.call_count == len(enqueued_tasks) + 1

    def test_run_stops_processing_after_max_tasks_is_reached(self, mock_close_old_connections: mock.Mock) -> None:
        """Test if run stops processing tasks once the max_tasks limit is reached."""
        for _ in range(5):
            _noop_task.enqueue()
        worker = Worker("w-00", "default", interval=0)
        stop = asyncio.Event()
        counter_yields = [None, None]

        def dummy_counter() -> Iterator[None]:
            for i, _ in enumerate(counter_yields):
                if i == len(counter_yields) - 1:
                    stop.set()
                yield

        with mock.patch.object(worker, "run_task") as mock_run_task:
            async_to_sync(worker.run)(
                ["*"],
                batch=True,
                stop_sign=stop,
                task_counter=dummy_counter(),
            )
        assert mock_run_task.await_count == len(counter_yields)
        assert mock_close_old_connections.call_count == len(counter_yields)

    def test_run_stops_processing_when_stop_sign_is_set(self, mock_close_old_connections: mock.Mock) -> None:
        """Test if run exits immediately when stop_sign is already set, without processing tasks."""
        _noop_task.enqueue()
        worker = Worker("w-00", "default", interval=0)
        stop_sign = asyncio.Event()
        stop_sign.set()
        with mock.patch.object(worker, "run_task") as mock_run_task:
            async_to_sync(worker.run)(
                ["*"],
                batch=False,
                stop_sign=stop_sign,
                task_counter=itertools.repeat(None),
            )
        mock_run_task.assert_not_awaited()
        mock_close_old_connections.assert_not_called()

    def test_run_non_batch_sleeps_interval_when_queue_is_empty(self) -> None:
        """Test if run sleeps for the configured interval when no task is found in the queue."""
        interval = 5.0
        worker = Worker("w-00", "default", interval=interval)
        stop = asyncio.Event()
        slept = False

        async def sleep_and_stop(_duration: float) -> None:
            nonlocal slept
            if not slept:
                slept = True
            else:
                stop.set()

        with mock.patch("asyncio.sleep", side_effect=sleep_and_stop) as mock_sleep:
            async_to_sync(worker.run)(
                ["*"],
                batch=False,
                stop_sign=stop,
                task_counter=itertools.repeat(None),
            )
        assert mock_sleep.await_args_list == [mock.ANY, mock.call(pytest.approx(interval))]

    def test_run_non_batch_does_not_sleep_interval_after_processing_task(self) -> None:
        """Test if run does not sleep for the configured interval after a task is processed."""
        _noop_task.enqueue()
        worker = Worker("w-00", "default", interval=5.0)
        stop = asyncio.Event()

        def dummy_counter() -> Iterator[None]:
            stop.set()
            yield

        with mock.patch("asyncio.sleep") as mock_sleep:
            async_to_sync(worker.run)(
                ["*"],
                batch=False,
                stop_sign=stop,
                task_counter=dummy_counter(),
            )

        mock_sleep.assert_awaited_once()

    @pytest.fixture
    def span_exporter(self) -> Generator[InMemorySpanExporter, Any]:
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        with mock.patch(
            "django_tasks_db_async.management.commands.db_async_worker.tracer",
            provider.get_tracer("test"),
        ):
            yield exporter

    # --- run_task ---

    def test_run_task_successful_task_marks_as_successful_stores_return_value_and_sends_signal(
        self,
        span_exporter: InMemorySpanExporter,
    ) -> None:
        """Test if run_task marks as SUCCESSFUL, stores the return value, sends signal and records a span."""
        worker = Worker("w-00", "default", interval=0)
        db_task_result = DBTaskResult.objects.get(id=_noop_task.enqueue().id)
        db_task_result.claim(worker.worker_id)
        with mock.patch.object(task_finished, "asend") as mock_signal:
            async_to_sync(worker.run_task)(db_task_result)
        db_task_result.refresh_from_db()
        assert db_task_result.status == TaskResultStatus.SUCCESSFUL
        assert db_task_result.return_value == "ok"
        assert worker._tasks_run == 1  # noqa: SLF001
        mock_signal.assert_awaited_once()
        [span] = span_exporter.get_finished_spans()
        expected_operation_name = f"{_noop_task.func.__module__}.{_noop_task.func.__qualname__}"
        assert span.name == "default process"
        assert span.kind == trace.SpanKind.CONSUMER
        assert span.attributes["messaging.system"] == "django_tasks_db"
        assert span.attributes["messaging.operation.type"] == "process"
        assert span.attributes["messaging.destination.name"] == "default"
        assert span.attributes["messaging.message.id"] == str(db_task_result.id)
        assert span.attributes["messaging.client.id"] == "w-00"
        assert span.attributes["messaging.operation.name"] == expected_operation_name

    def test_run_task_failed_task_marks_as_failed_records_exception_and_sends_signal(
        self,
        span_exporter: InMemorySpanExporter,
    ) -> None:
        """Test if run_task marks as FAILED, sends task_finished, records exception and sets span to ERROR."""
        worker = Worker("w-00", "default", interval=0)
        db_task_result = DBTaskResult.objects.get(id=_failing_task.enqueue().id)
        db_task_result.claim(worker.worker_id)
        with mock.patch.object(task_finished, "asend") as mock_signal:
            async_to_sync(worker.run_task)(db_task_result)
        db_task_result.refresh_from_db()
        assert db_task_result.status == TaskResultStatus.FAILED
        assert worker._tasks_run == 1  # noqa: SLF001
        mock_signal.assert_awaited_once()
        [span] = span_exporter.get_finished_spans()
        assert span.status.status_code == trace.StatusCode.ERROR
        assert len(span.events) == 1
        assert span.events[0].name == "exception"
        assert span.events[0].attributes["exception.type"] == "ValueError"

    def test_run_task_task_path_not_importable_does_not_send_signal(self) -> None:
        """Test if run_task does not send task_finished when task_path is not importable."""
        worker = Worker("w-00", "default", interval=0)
        db_task_result = DBTaskResult.objects.get(id=_noop_task.enqueue().id)
        db_task_result.task_path = "nonexistent.module.deleted_function"
        db_task_result.claim(worker.worker_id)
        with mock.patch.object(task_finished, "asend") as mock_signal:
            async_to_sync(worker.run_task)(db_task_result)
        mock_signal.assert_not_awaited()


class TestCommand:
    def test_ahandle_splits_comma_separated_queue_names(self) -> None:
        """Test if ahandle splits a comma-separated queue_name into individual queue names."""
        with mock.patch.object(Worker, "run") as mock_run:
            async_to_sync(Command().ahandle)(
                queue_name="queue-a,queue-b",
                interval=0,
                batch=True,
                backend_name="default",
                max_tasks=1,
                worker_id="test-worker",
                concurrency=1,
            )
        mock_run.assert_awaited_once_with(
            ["queue-a", "queue-b"],
            batch=True,
            stop_sign=mock.ANY,
            task_counter=mock.ANY,
        )
        stop_sign = mock_run.await_args_list[0].kwargs["stop_sign"]
        task_counter = mock_run.await_args_list[0].kwargs["task_counter"]

        assert isinstance(stop_sign, asyncio.Event)
        assert stop_sign.is_set() is False

        assert isinstance(task_counter, Iterator)
        assert next(task_counter) is None
        assert stop_sign.is_set() is True

    def test_ahandle_creates_concurrency_number_of_workers(self) -> None:
        """Test if ahandle spawns exactly as many workers as specified by concurrency."""
        concurrency = 3
        with mock.patch.object(Worker, "run") as mock_run:
            async_to_sync(Command().ahandle)(
                queue_name="default",
                interval=0,
                batch=True,
                backend_name="default",
                max_tasks=0,
                worker_id="test-worker",
                concurrency=concurrency,
            )
        assert mock_run.await_count == concurrency
