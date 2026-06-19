"""Tests for the db_async_worker management command."""
import asyncio
from collections.abc import Callable, Generator
from typing import Any
from unittest import mock

import pytest
from asgiref.sync import async_to_sync
from django.dispatch import Signal
from django.test import override_settings
from django_tasks_db.models import DBTaskResult
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from django_tasks_db_async._compat import TaskResultStatus, task, task_finished, task_started
from django_tasks_db_async.management.commands.db_async_worker import Command, Supervisor, Worker

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

    # --- Worker.run ---

    def test_run_processes_task_from_queue(self, mock_close_old_connections: mock.Mock) -> None:
        """Test that run picks a task from the work queue, executes it, and signals task_done."""
        db_task_result = DBTaskResult.objects.get(id=_noop_task.enqueue().id)
        worker = Worker("w-00", "default")
        queue: asyncio.Queue[DBTaskResult] = asyncio.Queue()
        queue.put_nowait(db_task_result)

        async def run_task_and_stop(task_result: DBTaskResult) -> None:
            worker.stop()

        with mock.patch.object(worker, "run_task", side_effect=run_task_and_stop) as mock_run_task:
            async_to_sync(worker.run)(queue)

        mock_run_task.assert_awaited_once_with(db_task_result)
        assert queue.empty()
        mock_close_old_connections.assert_called_once()

    def test_run_stops_immediately_when_stop_called_before_start(
        self,
        mock_close_old_connections: mock.Mock,
    ) -> None:
        """Test that run exits without processing any task if stop() is called before run."""
        _noop_task.enqueue()
        worker = Worker("w-00", "default")
        queue: asyncio.Queue[DBTaskResult] = asyncio.Queue()
        worker.stop()

        with mock.patch.object(worker, "run_task") as mock_run_task:
            async_to_sync(worker.run)(queue)

        mock_run_task.assert_not_awaited()
        mock_close_old_connections.assert_not_called()

    # --- Worker.run_task ---

    @pytest.fixture
    def signal_collector(self) -> Generator[Callable[[Signal], list], Any]:
        connected: list[tuple[Signal, Any]] = []

        def collect(signal: Signal) -> list:
            received: list = []

            def receiver(sender: object, **kwargs: object) -> None:
                received.append({"sender": sender, **kwargs})

            signal.connect(receiver, weak=False)
            connected.append((signal, receiver))
            return received

        yield collect

        for signal, receiver in connected:
            signal.disconnect(receiver)

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

    def test_run_task_successful_task_marks_as_successful_stores_return_value_and_sends_signal(
        self,
        span_exporter: InMemorySpanExporter,
        signal_collector: Callable[[Signal], list],
    ) -> None:
        """Test if run_task marks as SUCCESSFUL, stores the return value, sends signal and records a span."""
        started = signal_collector(task_started)
        finished = signal_collector(task_finished)
        worker = Worker("w-00", "default")
        db_task_result = DBTaskResult.objects.get(id=_noop_task.enqueue().id)
        db_task_result.claim(worker.worker_id)
        async_to_sync(worker.run_task)(db_task_result)
        db_task_result.refresh_from_db()
        assert db_task_result.status == TaskResultStatus.SUCCESSFUL
        assert db_task_result.return_value == "ok"
        assert worker._tasks_run == 1  # noqa: SLF001
        assert len(started) == 1
        assert len(finished) == 1
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
        signal_collector: Callable[[Signal], list],
    ) -> None:
        """Test if run_task marks as FAILED, sends task_finished, records exception and sets span to ERROR."""
        started = signal_collector(task_started)
        finished = signal_collector(task_finished)
        worker = Worker("w-00", "default")
        db_task_result = DBTaskResult.objects.get(id=_failing_task.enqueue().id)
        db_task_result.claim(worker.worker_id)
        async_to_sync(worker.run_task)(db_task_result)
        db_task_result.refresh_from_db()
        assert db_task_result.status == TaskResultStatus.FAILED
        assert worker._tasks_run == 1  # noqa: SLF001
        assert len(started) == 1
        assert len(finished) == 1
        [span] = span_exporter.get_finished_spans()
        assert span.status.status_code == trace.StatusCode.ERROR
        assert len(span.events) == 1
        assert span.events[0].name == "exception"
        assert span.events[0].attributes["exception.type"] == "ValueError"

    def test_run_task_task_path_not_importable_does_not_send_signal(
        self,
        signal_collector: Callable[[Signal], list],
    ) -> None:
        """Test if run_task does not send task_started or task_finished when task_path is not importable."""
        started = signal_collector(task_started)
        finished = signal_collector(task_finished)
        worker = Worker("w-00", "default")
        db_task_result = DBTaskResult.objects.get(id=_noop_task.enqueue().id)
        db_task_result.task_path = "nonexistent.module.deleted_function"
        db_task_result.claim(worker.worker_id)
        async_to_sync(worker.run_task)(db_task_result)
        assert not started
        assert not finished


class TestSupervisor:
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

    def test_batch_mode_exits_immediately_when_no_tasks_available(self) -> None:
        """Test that supervisor in batch mode exits without processing when the queue is empty."""
        with mock.patch.object(Worker, "run_task") as mock_run_task:
            async_to_sync(Supervisor("default", ["*"], "test-worker").run)(
                concurrency=1, interval=0, batch=True
            )
        mock_run_task.assert_not_awaited()

    def test_batch_mode_processes_all_available_tasks_then_exits(self) -> None:
        """Test that supervisor processes all available tasks in batch mode then exits."""
        enqueued = [_noop_task.enqueue(), _noop_task.enqueue()]
        with mock.patch.object(Worker, "run_task") as mock_run_task:
            async_to_sync(Supervisor("default", ["*"], "test-worker").run)(
                concurrency=1, interval=0, batch=True
            )
        assert mock_run_task.await_count == len(enqueued)

    def test_wildcard_queue_processes_tasks_from_any_queue(self) -> None:
        """Test that queue_names=['*'] processes tasks regardless of their queue."""
        enqueued = [_noop_task.using(queue_name="other").enqueue()]
        with mock.patch.object(Worker, "run_task") as mock_run_task:
            async_to_sync(Supervisor("default", ["*"], "test-worker").run)(
                concurrency=1, interval=0, batch=True
            )
        assert mock_run_task.await_count == len(enqueued)

    def test_specific_queue_skips_tasks_in_other_queues(self) -> None:
        """Test that a specific queue_name does not process tasks from other queues."""
        _noop_task.using(queue_name="other").enqueue()
        with mock.patch.object(Worker, "run_task") as mock_run_task:
            async_to_sync(Supervisor("default", ["default"], "test-worker").run)(
                concurrency=1, interval=0, batch=True
            )
        mock_run_task.assert_not_awaited()

    def test_max_tasks_limits_total_tasks_processed(self) -> None:
        """Test that max_tasks caps the number of tasks processed across all workers."""
        max_tasks = 2
        for _ in range(5):
            _noop_task.enqueue()
        with mock.patch.object(Worker, "run_task") as mock_run_task:
            async_to_sync(Supervisor("default", ["*"], "test-worker", max_tasks=max_tasks).run)(
                concurrency=1, interval=0, batch=True
            )
        assert mock_run_task.await_count == max_tasks

    def test_non_batch_sleeps_interval_when_no_tasks_available(self) -> None:
        """Test that the puller sleeps for the configured interval when no tasks are found."""
        interval = 5.0
        supervisor = Supervisor("default", ["*"], "test-worker")
        call_count = 0

        async def sleep_and_eventually_stop(duration: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                supervisor.request_shutdown()

        with mock.patch("asyncio.sleep", side_effect=sleep_and_eventually_stop) as mock_sleep:
            async_to_sync(supervisor.run)(concurrency=1, interval=interval, batch=False)

        assert any(
            call == mock.call(pytest.approx(interval)) for call in mock_sleep.await_args_list
        )

    def test_request_shutdown_stops_processing_after_current_tasks_finish(self) -> None:
        """Test that request_shutdown stops the puller and lets in-flight tasks complete."""
        supervisor = Supervisor("default", ["*"], "test-worker")
        tasks_processed = 0

        async def run_task_and_shutdown(db_task_result: DBTaskResult) -> None:
            nonlocal tasks_processed
            tasks_processed += 1
            supervisor.request_shutdown()

        _noop_task.enqueue()
        _noop_task.enqueue()

        with mock.patch.object(Worker, "run_task", side_effect=run_task_and_shutdown):
            async_to_sync(supervisor.run)(concurrency=1, interval=0, batch=False)

        assert tasks_processed == 1


class TestCommand:
    def test_ahandle_splits_comma_separated_queue_names(self) -> None:
        """Test that ahandle parses comma-separated queue_name into a list for the Supervisor."""
        with mock.patch(
            "django_tasks_db_async.management.commands.db_async_worker.Supervisor",
        ) as MockSupervisor:
            MockSupervisor.return_value.run = mock.AsyncMock()
            async_to_sync(Command().ahandle)(
                queue_name="queue-a,queue-b",
                interval=0,
                batch=True,
                backend_name="default",
                max_tasks=None,
                worker_id="test-worker",
                concurrency=1,
            )
        MockSupervisor.assert_called_once_with("default", ["queue-a", "queue-b"], "test-worker", None)
        MockSupervisor.return_value.run.assert_awaited_once_with(concurrency=1, interval=0, batch=True)

    def test_ahandle_passes_concurrency_to_supervisor_run(self) -> None:
        """Test that ahandle forwards the concurrency argument to supervisor.run."""
        concurrency = 3
        with mock.patch(
            "django_tasks_db_async.management.commands.db_async_worker.Supervisor",
        ) as MockSupervisor:
            MockSupervisor.return_value.run = mock.AsyncMock()
            async_to_sync(Command().ahandle)(
                queue_name="default",
                interval=0,
                batch=True,
                backend_name="default",
                max_tasks=None,
                worker_id="test-worker",
                concurrency=concurrency,
            )
        MockSupervisor.return_value.run.assert_awaited_once_with(
            concurrency=concurrency, interval=0, batch=True
        )
