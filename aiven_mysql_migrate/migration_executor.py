# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate.enums import MySQLMigrateTool
from aiven_mysql_migrate.exceptions import MySQLDumpException, MySQLImportException
from aiven_mysql_migrate.utils import MySQLConnectionInfo, MySQLDumpProcessor, select_global_var, MydumperDumpProcessor
from concurrent import futures
from pathlib import Path
from subprocess import Popen
from typing import Callable, List, Optional

import concurrent
import logging
import resource
import subprocess
import sys

LOGGER = logging.getLogger(__name__)


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
        dump_output_dir: Optional[Path] = None,
        backup_dir: Optional[Path] = None
    ) -> Optional[str]:
        """
        Execute dump and import commands with piping.

        Args:
            dump_cmd: The dump command and arguments
            import_cmd: The import command and arguments
            target: Target database connection info
            line_processor: Optional function to process each line from dump output
            dump_tool: The dump tool being used ("mysqldump" or "mydumper")
            dump_output_dir: Optional path to dump output directory (for mydumper)
            backup_dir: Optional path to backup directory (for mydumper metadata files)

        Returns:
            Tuple of (dump_exit_code, import_exit_code, extracted_gtid)
        """
        LOGGER.info("Starting import from source to target database")
        # Choose the appropriate dump processor based on the dump tool
        dump_processor: Optional[MySQLDumpProcessor | MydumperDumpProcessor]
        if line_processor:
            dump_processor = None
        elif dump_tool == MySQLMigrateTool.mydumper:
            assert dump_output_dir is not None and backup_dir is not None, (
                "dump_output_dir and backup_dir must be provided when using mydumper"
            )

            dump_processor = MydumperDumpProcessor(
                dump_output_dir=dump_output_dir,
                backup_dir=backup_dir
            )
        else:  # mysqldump (default)
            dump_processor = MySQLDumpProcessor()
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
            self.import_proc.stdin.close()

        def _reader_stderr(proc):
            for line in proc.stderr:
                sys.stderr.write(line)

        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            for future in concurrent.futures.as_completed([
                executor.submit(_reader_stdout),
                executor.submit(_reader_stderr, self.dump_proc),
                executor.submit(_reader_stderr, self.import_proc)
            ]):
                future.result()

        export_code = self.dump_proc.wait()
        import_code = self.import_proc.wait()

        if export_code != 0:
            raise MySQLDumpException(f"Error while exporting data from the source database, exit code: {export_code}")

        if import_code != 0:
            raise MySQLImportException(f"Error while importing data into the target database, exit code: {import_code}")

        gtid = dump_processor.get_gtid() if dump_processor else None
        return gtid

    def terminate_processes(self) -> None:
        for proc in (self.import_proc, self.dump_proc):
            if proc:
                LOGGER.warning("Terminating subprocess with pid: %s", proc.pid)
                proc.kill()
