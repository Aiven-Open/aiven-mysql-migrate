# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate.enums import MySQLMigrateTool
from aiven_mysql_migrate.exceptions import MySQLDumpException, MySQLImportException
from aiven_mysql_migrate.utils import MySQLConnectionInfo, DumpProcessor, select_global_var
from collections import deque
from concurrent import futures
from subprocess import Popen
from typing import Callable, Deque, List, Optional

import concurrent
import logging
import re
import resource
import subprocess
import sys

LOGGER = logging.getLogger(__name__)

STDERR_TAIL_LINES = 100
STDERR_TAIL_MAX_BYTES = 2048
STDERR_FALLBACK_LINES = 20
# mydumper/myloader surface fatal worker failures via GLib's g_critical()
# ("** (tool:pid): CRITICAL **: ...") and the MySQL client library reports
# server errors as "ERROR NNNN". When a process exits non-zero, we prefer
# those lines over generic log noise when building the user-visible message.
_STDERR_SIGNAL_RE = re.compile(r"CRITICAL \*\*:|ERROR \d{3,4}")


def _format_stderr_tail(buf: "Deque[str]") -> str:
    if not buf:
        return ""
    signal_lines = [line for line in buf if _STDERR_SIGNAL_RE.search(line)]
    selected = signal_lines if signal_lines else list(buf)[-STDERR_FALLBACK_LINES:]
    tail = "".join(selected).strip()
    if len(tail) > STDERR_TAIL_MAX_BYTES:
        tail = "..." + tail[-(STDERR_TAIL_MAX_BYTES - 3):]
    return tail


class ProcessExecutor:
    """Responsible for executing external processes with piping."""

    def __init__(self) -> None:
        self.import_proc: Optional[Popen] = None
        self.dump_proc: Optional[Popen] = None

    def execute_piped_commands(
        self,
        dump_cmd: List[str],
        import_cmd: List[str],
        target: MySQLConnectionInfo,
        *,
        line_processor: Optional[Callable[[str], str]] = None,
        dump_tool: MySQLMigrateTool = MySQLMigrateTool.mysqldump,
        dump_processor: Optional[DumpProcessor] = None
    ) -> Optional[str]:
        """
        Execute dump and import commands with piping.

        Args:
            dump_cmd: The dump command and arguments
            import_cmd: The import command and arguments
            target: Target database connection info
            line_processor: Optional function to process each line from dump output
            dump_tool: The dump tool being used ("mysqldump" or "mydumper")
            dump_processor: Optional dump processor for processing dump output lines

        Returns:
            Tuple of (dump_exit_code, import_exit_code, extracted_gtid)
        """
        LOGGER.info("Starting import from source to target database")
        self.dump_proc = Popen(  # pylint: disable=consider-using-with
            dump_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        self.import_proc = Popen(  # pylint: disable=consider-using-with
            import_cmd,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Disallow creating child processes in migration target when this runs as non-root user
        if hasattr(resource, "prlimit") and dump_tool == MySQLMigrateTool.mysqldump:
            resource.prlimit(self.import_proc.pid, resource.RLIMIT_NPROC, (0, 0))  # pylint: disable=no-member

        # make mypy happy
        assert self.dump_proc.stdout
        assert self.dump_proc.stderr
        assert self.import_proc.stdin

        # If sql_require_primary_key is ON globally - it's not possible to import tables without a primary key
        with target.cur() as cur:
            if select_global_var(cur, "sql_require_primary_key") == 1:
                self.import_proc.stdin.write("SET SESSION sql_require_primary_key = 0;")

        def _reader_stdout():
            for line in self.dump_proc.stdout:
                if line_processor:
                    processed_line = line_processor(line)
                else:
                    processed_line = dump_processor.process_line(line)

                if not processed_line:
                    continue

                LOGGER.debug("dump: %s", processed_line)
                self.import_proc.stdin.write(processed_line + "\n")
                self.import_proc.stdin.flush()
                for handler in logging.getHandlerNames():
                    logging.getHandlerByName(handler).flush()
                sys.stdout.flush()

            self.import_proc.stdin.flush()
            self.import_proc.stdin.close()

        dump_stderr_tail: Deque[str] = deque(maxlen=STDERR_TAIL_LINES)
        import_stderr_tail: Deque[str] = deque(maxlen=STDERR_TAIL_LINES)

        def _reader_stderr(proc, sink: Deque[str]):
            # Once we've captured a critical line (CRITICAL / ERROR NNNN), stop
            # appending non-critical lines so a subsequent flood of warnings
            # can't evict the critical from the bounded deque.
            critical_seen = False
            for line in proc.stderr:
                sys.stderr.write(line)
                if (is_critical := _STDERR_SIGNAL_RE.search(line)) or not critical_seen:
                    sink.append(line)
                if is_critical:
                    critical_seen = True

        reader_exc: Optional[BaseException] = None
        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            submitted = [
                executor.submit(_reader_stdout),
                executor.submit(_reader_stderr, self.dump_proc, dump_stderr_tail),
                executor.submit(_reader_stderr, self.import_proc, import_stderr_tail),
            ]
            for future in concurrent.futures.as_completed(submitted):
                try:
                    future.result()
                except BaseException as exc:  # pylint: disable=broad-except
                    LOGGER.debug("Reader task raised: %s", exc)
                    if reader_exc is None:
                        reader_exc = exc

        export_code = self.dump_proc.wait()
        import_code = self.import_proc.wait()

        if export_code != 0:
            tail = _format_stderr_tail(dump_stderr_tail)
            msg = f"Error while exporting data from the source database, exit code: {export_code}"
            if tail:
                msg += f". Last stderr: {tail}"
            raise MySQLDumpException(msg)

        if import_code != 0:
            tail = _format_stderr_tail(import_stderr_tail)
            msg = f"Error while importing data into the target database, exit code: {import_code}"
            if tail:
                msg += f". Last stderr: {tail}"
            raise MySQLImportException(msg)

        if reader_exc is not None:
            raise reader_exc

        gtid = dump_processor.get_gtid() if dump_processor else None
        return gtid

    def terminate_processes(self) -> None:
        for proc in (self.import_proc, self.dump_proc):
            if proc:
                LOGGER.warning("Terminating subprocess with pid: %s", proc.pid)
                proc.kill()
