import sys
from typing import Any

import django
from django.dispatch import Signal

if django.VERSION >= (5, 0):

    async def dispatch_signal(signal: Signal, sender: type[Any], **named: Any) -> Any:  # noqa: ANN401
        return await signal.asend(sender, **named)
else:
    from asgiref.sync import sync_to_async

    async def dispatch_signal(signal: Signal, sender: type[Any], **named: Any) -> Any:  # noqa: ANN401
        return await sync_to_async(signal.send)(sender, **named)


if django.VERSION >= (6, 0):
    from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, TaskResult, task
    from django.tasks.base import DEFAULT_TASK_QUEUE_NAME, TaskContext, TaskResultStatus
    from django.tasks.signals import task_finished, task_started

else:
    from django_tasks import DEFAULT_TASK_BACKEND_ALIAS, TaskResult, task
    from django_tasks.base import DEFAULT_TASK_QUEUE_NAME, TaskContext, TaskResultStatus
    from django_tasks.signals import task_finished, task_started

if sys.version_info >= (3, 11):
    from asyncio import TaskGroup

    BaseExceptionGroup = BaseExceptionGroup  # noqa: F821, PLW0127
else:
    from exceptiongroup import BaseExceptionGroup
    from taskgroup import TaskGroup


__all__ = [
    "DEFAULT_TASK_BACKEND_ALIAS",
    "DEFAULT_TASK_QUEUE_NAME",
    "BaseExceptionGroup",
    "TaskContext",
    "TaskGroup",
    "TaskResult",
    "TaskResultStatus",
    "dispatch_signal",
    "task",
    "task_finished",
    "task_started",
]
