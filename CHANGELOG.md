# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Refreshed project metadata and test coverage for Django 5.2 through 6.1 and Python
  3.10 through 3.14.

### Fixed

- Declared the missing direct runtime dependencies and added a `deptry` check to CI.
- Added compatibility shims for `TaskGroup` and signal dispatch across supported Django and
  Python versions.

### Added

- Added workspace-level Python configuration to streamline local development and testing.

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
- Support for Django 5.2, 6.0 and 6.1.
- Support for Python 3.10, 3.11, 3.12, 3.13, and 3.14.

### Fixed

- Added compatibility support for `TaskGroup` on Python 3.10 via the backport package.
- Correct signal dispatching on Django versions prior to 5.0 by using sync send.

### Changed

- Extracted the Django task compatibility layer into a dedicated `_compat` module.
- Improved linting and project metadata declarations.

[Unreleased]: https://github.com/olist/django-tasks-db-async/compare/0.0.1...HEAD
[0.0.1]: https://github.com/olist/django-tasks-db-async/releases/tag/0.0.1
