# django-tasks-db-async

[![PyPI - Version](https://img.shields.io/pypi/v/django-tasks-db-async.svg)](https://pypi.org/project/django-tasks-db-async)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/django-tasks-db-async.svg)](https://pypi.org/project/django-tasks-db-async)

Async worker for [django-tasks-db](https://github.com/RealOrangeOne/django-tasks-db) with OpenTelemetry instrumentation.

-----

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Running the worker](#running-the-worker)
- [OpenTelemetry](#opentelemetry)
- [License](#license)

## Installation

```console
pip install django-tasks-db-async
```

## Configuration

Add both `django_tasks_db` and `django_tasks_db_async` to `INSTALLED_APPS`, then configure the `TASKS` setting to use `django_tasks_db.DatabaseBackend`:

```python
INSTALLED_APPS = [
    ...
    "django_tasks_db",
    "django_tasks_db_async",
]

TASKS = {
    "default": {
        "BACKEND": "django_tasks_db.DatabaseBackend",
        "QUEUES": ["default"],
    },
}
```

Run the database migrations to create the task result table:

```console
python manage.py migrate
```

## Running the worker

Start the worker with the `db_async_worker` management command:

```console
python manage.py db_async_worker
```

### Options

| Option | Default | Description |
|---|---|---|
| `--queue-name` | `default` | Queue(s) to process. Separate multiple with a comma. Use `*` to process all queues. |
| `--interval` | `1` | Seconds to wait when the queue is empty before polling again. |
| `--concurrency` | `10` | Number of concurrent worker coroutines. |
| `--batch` | `False` | Process all outstanding tasks and exit. |
| `--max-tasks` | unlimited | Maximum number of tasks each worker will execute before stopping. |
| `--backend` | `default` | Backend alias to operate on (as configured in `TASKS`). |
| `--worker-id` | auto | Worker identifier, must be unique across the pool. |

> [!NOTE]
> Each worker coroutine holds one database connection for the duration of its polling loop.
> A `--concurrency` of 10 (the default) can therefore open up to 10 simultaneous connections.
> Make sure your database connection pool (or `CONN_MAX_AGE`) is sized accordingly, and consider
> lowering `--concurrency` if your database has a low connection limit.

### Examples

Process tasks from all queues with 4 concurrent workers:

```console
python manage.py db_async_worker --queue-name='*' --concurrency=4
```

Process tasks from specific queues:

```console
python manage.py db_async_worker --queue-name=default,emails,notifications
```

Run in batch mode — process outstanding tasks and exit (useful for cron jobs):

```console
python manage.py db_async_worker --batch
```

### Signals

The worker dispatches the standard `task_started` and `task_finished` Django signals around each task execution, compatible with both `django.tasks` (Django 6.0+) and the standalone `django-tasks` package.

## OpenTelemetry

The worker records a span for each task execution using the [OpenTelemetry messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/). No additional configuration is required — the worker picks up whatever `TracerProvider` is active in the process.

Each span includes the following attributes:

| Attribute | Value |
|---|---|
| `messaging.system` | `django_tasks_db` |
| `messaging.operation.type` | `process` |
| `messaging.destination.name` | queue name |
| `messaging.message.id` | task result UUID |
| `messaging.client.id` | worker ID |
| `messaging.operation.name` | fully qualified task function name |

Failed tasks set the span status to `ERROR` and record the exception as a span event.

## License

`django-tasks-db-async` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
