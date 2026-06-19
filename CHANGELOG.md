# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Worker pool is now managed by a `Supervisor` class that owns task claiming, queue feeding,
  batch mode, max-tasks tracking, and graceful/forced shutdown. `Worker` is simplified to
  only execute tasks received from a shared queue.
- Task claiming is now batched: the `Supervisor` issues a single `SELECT FOR UPDATE SKIP
  LOCKED` per polling cycle to claim up to `concurrency` tasks at once, instead of each
  worker issuing its own query. This reduces database load under high concurrency.

### Fixed

- Declared all missing direct dependencies and added `deptry` to CI to prevent regressions.

## [0.0.1] - 2026-04-30

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

### Fixed

- Signal dispatch compatibility with Django < 5.0 (used sync `send` wrapped in
  `sync_to_async` instead of the async-only `asend`).
- `TaskGroup` compatibility on Python 3.10 via the `taskgroup` backport.

[unreleased]: https://github.com/olist/django-tasks-db-async/compare/0.0.1...HEAD
[0.0.1]: https://github.com/olist/django-tasks-db-async/releases/tag/0.0.1
