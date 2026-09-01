import sys

import django

if django.VERSION >= (6, 0):
    from django.tasks import task

else:
    from django_tasks import task

if sys.version_info >= (3, 11):
    from asyncio import TaskGroup
else:
    from taskgroup import TaskGroup


__all__ = [
    "TaskGroup",
    "task",
]
