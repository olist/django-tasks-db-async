import django

if django.VERSION >= (6, 0):
    from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, TaskResult, task
    from django.tasks.base import DEFAULT_TASK_QUEUE_NAME, TaskContext, TaskResultStatus
    from django.tasks.signals import task_finished, task_started
else:
    from django_tasks import DEFAULT_TASK_BACKEND_ALIAS, TaskResult, task
    from django_tasks.base import DEFAULT_TASK_QUEUE_NAME, TaskContext, TaskResultStatus
    from django_tasks.signals import task_finished, task_started

__all__ = [
    "DEFAULT_TASK_BACKEND_ALIAS",
    "DEFAULT_TASK_QUEUE_NAME",
    "TaskContext",
    "TaskResult",
    "TaskResultStatus",
    "task",
    "task_finished",
    "task_started",
]
