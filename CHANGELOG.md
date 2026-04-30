# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `db_async_worker` management command with configurable queue names, polling interval,
  batch mode, concurrency, max-tasks limit, and graceful SIGTERM/SIGINT shutdown.
- Atomic task claiming using `SELECT FOR UPDATE SKIP LOCKED` to avoid double-processing
  under concurrent workers.
- OpenTelemetry instrumentation: each task execution produces a `CONSUMER` span following
  the messaging semantic conventions (`messaging.system`, `messaging.destination.name`,
  `messaging.message.id`, `messaging.operation.name`, etc.).
- `task_started` and `task_finished` signals dispatched around every task execution.
- Support for Django 4.2, 5.0, 5.1, 5.2, and 6.0.
- Support for Python 3.10, 3.11, 3.12, 3.13, and 3.14.

[unreleased]: https://github.com/olist/django-tasks-db-async/compare/HEAD...HEAD
