# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate import migration_executor
from aiven_mysql_migrate.enums import MySQLMigrateTool
from aiven_mysql_migrate.exceptions import MySQLDumpException, MySQLImportException
from aiven_mysql_migrate.migration_executor import ProcessExecutor, STDERR_TAIL_LINES, _format_stderr_tail
from aiven_mysql_migrate.utils import MySQLConnectionInfo, MydumperDumpProcessor
from collections import deque
from contextlib import contextmanager
from pytest import fixture, raises
from typing import Optional
from unittest.mock import MagicMock, patch


_MYLOADER_CRITICAL = (
    "** (myloader:1): CRITICAL **: 00:00:00.000: Thread 1 using connection 5 - "
    "ERROR 3750: Unable to create or change a table without a primary key, when the "
    "system variable 'sql_require_primary_key' is set.\n"
)


class _StubStdin:
    """Writable stdin that can be scripted to raise BrokenPipeError."""

    def __init__(self, *, raise_on_write_after: Optional[int] = None):
        self._raise_after = raise_on_write_after
        self._writes = 0
        self.closed = False

    def write(self, _data):
        self._writes += 1
        if self._raise_after is not None and self._writes > self._raise_after:
            raise BrokenPipeError(32, "Broken pipe")

    def flush(self):
        if self._raise_after is not None and self._writes > self._raise_after:
            raise BrokenPipeError(32, "Broken pipe")

    def close(self):
        self.closed = True


def _stub_proc(*, stdout_lines=(), stderr_lines=(), exit_code=0, stdin=None):
    proc = MagicMock()
    proc.stdout = iter(list(stdout_lines))
    proc.stderr = iter(list(stderr_lines))
    proc.stdin = stdin if stdin is not None else _StubStdin()
    proc.wait.return_value = exit_code
    proc.pid = 4242
    return proc


@fixture(name="target_connection")
def _target_connection():
    conn = MagicMock(spec=MySQLConnectionInfo)

    @contextmanager
    def _cur():
        yield MagicMock()

    conn.cur.side_effect = _cur
    return conn


@fixture(name="dump_processor")
def _dump_processor():
    processor = MagicMock(spec=MydumperDumpProcessor)
    # Each dump-stdout line flows through process_line; default pass-through.
    processor.process_line.side_effect = lambda line: line.rstrip("\n") if line.strip() else ""
    processor.get_gtid.return_value = None
    return processor


def _run_executor(dump_proc, import_proc, target_connection, dump_processor):
    with patch.object(migration_executor, "Popen", side_effect=[dump_proc, import_proc]), \
         patch.object(migration_executor, "select_global_var", return_value=0):
        executor = ProcessExecutor()
        return executor.execute_piped_commands(
            dump_cmd=["mydumper"],
            import_cmd=["myloader"],
            target=target_connection,
            dump_tool=MySQLMigrateTool.mydumper,
            dump_processor=dump_processor,
        )


def test_stderr_tail_surfaced_on_import_failure(target_connection, dump_processor):
    dump_proc = _stub_proc(stdout_lines=["schema.sql\n", "data.sql\n"], exit_code=0)
    # Loader dies on the second write — mirrors what happens in production when
    # a myloader worker hits ERROR 3750 and the loader process exits.
    import_proc = _stub_proc(
        stderr_lines=["loading schema\n", _MYLOADER_CRITICAL],
        exit_code=1,
        stdin=_StubStdin(raise_on_write_after=1),
    )

    with raises(MySQLImportException) as exc_info:
        _run_executor(dump_proc, import_proc, target_connection, dump_processor)

    msg = str(exc_info.value)
    assert "exit code: 1" in msg
    assert "ERROR 3750" in msg


def test_stderr_tail_surfaced_on_dump_failure(target_connection, dump_processor):
    mydumper_critical = (
        "** (mydumper:7): CRITICAL **: 00:00:00.000: Error connecting to database: "
        "ERROR 1045: Access denied for user 'u'@'h'\n"
    )
    dump_proc = _stub_proc(stderr_lines=[mydumper_critical], exit_code=2)
    import_proc = _stub_proc(exit_code=0)

    with raises(MySQLDumpException) as exc_info:
        _run_executor(dump_proc, import_proc, target_connection, dump_processor)

    msg = str(exc_info.value)
    assert "exit code: 2" in msg
    assert "ERROR 1045" in msg


def test_reader_exception_is_reraised_when_exit_codes_are_clean(target_connection, dump_processor):
    # Both subprocesses exit 0 but _reader_stdout raises (BrokenPipeError here).
    # Without re-raise, a reader-side code bug would be silently swallowed.
    dump_proc = _stub_proc(stdout_lines=["schema.sql\n"], exit_code=0)
    import_proc = _stub_proc(exit_code=0, stdin=_StubStdin(raise_on_write_after=0))

    with raises(BrokenPipeError):
        _run_executor(dump_proc, import_proc, target_connection, dump_processor)


def test_stderr_tail_bounded_preserves_signal_line(target_connection, dump_processor):
    noise = [f"loading file-{i}.sql\n" for i in range(STDERR_TAIL_LINES * 3)]
    # Put the critical line near the front and bury it under a warning flood
    # that exceeds the deque's maxlen. _reader_stderr stops appending
    # non-critical lines once a critical is seen, so the critical stays in the
    # deque and surfaces in the message.
    stderr_lines = [_MYLOADER_CRITICAL] + noise
    import_proc = _stub_proc(stderr_lines=stderr_lines, exit_code=1)
    dump_proc = _stub_proc(exit_code=0)

    with raises(MySQLImportException) as exc_info:
        _run_executor(dump_proc, import_proc, target_connection, dump_processor)

    msg = str(exc_info.value)
    assert "ERROR 3750" in msg  # critical preserved despite warning flood
    # Message size cap holds.
    assert len(msg) < 4096


def test_stderr_tail_fallback_when_no_pattern_match(target_connection, dump_processor):
    noisy_but_no_signal = [f"progress: {i}%\n" for i in range(5)]
    import_proc = _stub_proc(stderr_lines=noisy_but_no_signal, exit_code=3)
    dump_proc = _stub_proc(exit_code=0)

    with raises(MySQLImportException) as exc_info:
        _run_executor(dump_proc, import_proc, target_connection, dump_processor)

    msg = str(exc_info.value)
    assert "exit code: 3" in msg
    assert "progress:" in msg


def test_format_stderr_tail_empty_returns_empty_string():
    assert _format_stderr_tail(deque()) == ""


def test_format_stderr_tail_truncates_over_max_bytes():
    long_line = "ERROR 3750: " + ("x" * 5000) + "\n"
    tail = _format_stderr_tail(deque([long_line]))
    assert tail.startswith("...")
    assert len(tail) <= migration_executor.STDERR_TAIL_MAX_BYTES
