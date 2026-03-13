# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

`aiven-mysql-migrate` is a CLI tool for migrating MySQL databases between servers. It supports two methods:
- **replication**: mysqldump + GTID-based replication (preferred for production, minimal downtime)
- **dump**: mysqldump-only (fallback when replication preconditions aren't met)

Each method supports two dump tools: `mysqldump` (MySQL native) and `mydumper` (third-party, parallel).

## Build & Test Commands

```bash
# Install dev dependencies (requires Python 3.10-3.14)
pip install -e ".[dev]"

# Unit tests
make test
# or: python3 -m pytest -v test/unit/

# Run a single unit test
python3 -m pytest -v test/unit/test_utils.py::test_mysql_dump_processor_extract_gtid

# System tests (requires Docker/Podman; spins up MySQL 5.7/8.0/8.4 containers)
make systest
make systest DOCKER=podman

# Linting & type checking
make flake8
make pylint
make mypy
make static-checks   # all three above

# Auto-format
make isort yapf
```

## Architecture

The migration flow is: `main.py` (CLI/arg parsing) -> `migration.py` (orchestration) -> `dump_tools.py` (dump/import commands) -> `migration_executor.py` (subprocess piping).

### Key modules in `aiven_mysql_migrate/`

- **`main.py`** — CLI entry point (`mysql_migrate` command). Parses args, reads env vars, calls `MySQLMigration`.
- **`migration.py`** — `MySQLMigration` class. Runs pre-checks (version compat, GTID mode, engine support, server IDs, grants, binlog format), then orchestrates dump -> import -> replication setup.
- **`dump_tools.py`** — `MySQLDumpTool` and `MyDumperTool` (both extend `MySQLMigrationToolBase`). Build CLI commands for mysqldump/mysql and mydumper/myloader respectively. Factory: `get_dump_tool()`.
- **`migration_executor.py`** — `ProcessExecutor`. Runs dump and import as piped subprocesses, with a `DumpProcessor` reading stdout line-by-line.
- **`utils.py`** — `MySQLConnectionInfo` (connection/URI parsing), `DumpProcessor` hierarchy (`MySQLDumpProcessor` for mysqldump, `MydumperDumpProcessor` for mydumper). Also contains regex patterns for GTID extraction, definer stripping, and SQL mode filtering.
- **`config.py`** — Constants and env var reads (`SOURCE_SERVICE_URI`, `TARGET_SERVICE_URI`, `TARGET_MASTER_SERVICE_URI`).
- **`enums.py`** — `MySQLMigrateTool` (mysqldump/mydumper) and `MySQLMigrateMethod` (dump/replication).
- **`exceptions.py`** — Exception hierarchy. `ReplicationNotAvailableException` subclasses cause fallback to dump method. `MigrationPreCheckException` subclasses are fatal pre-check failures.

### Tests

- **`test/unit/`** — Pure unit tests. `test_utils.py` covers dump processors, URI parsing, password validation. `test_dump_tools.py` covers command generation and temp file handling.
- **`test/sys/`** — System tests against real MySQL containers (via `docker-compose.test.yaml`). Tests run inside a container built from `Dockerfile`. The `db_name` fixture generates random 10-char alphanumeric names.

## Conventions

- SQL identifiers derived from variables (like `db_name`) must be backtick-quoted in queries to handle names starting with digits.
- The project uses `pymysql` with `DictCursor` — query results are dicts keyed by column name.
- Formatting: `yapf` + `isort`. Linting: `flake8` + `pylint`. Types: `mypy`.
- `setup.py` is a shim; all config is in `pyproject.toml`.
