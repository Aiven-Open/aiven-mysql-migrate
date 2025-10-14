# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
import textwrap
from abc import ABC, abstractmethod
from aiven_mysql_migrate.exceptions import DumpToolNotFoundError, ReplicaSetupException
from aiven_mysql_migrate.migration_executor import ProcessExecutor
from aiven_mysql_migrate.utils import MySQLConnectionInfo
from enum import Enum
from pathlib import Path
from typing import List, Optional

import logging
import os
import shlex
import subprocess
import tempfile

LOGGER = logging.getLogger(__name__)


class MySQLMigrateMethod(str, Enum):
    dump = "dump"
    replication = "replication"


class MySQLMigrateTool(str, Enum):
    mysqldump = "mysqldump"
    mydumper = "mydumper"


class MySQLMigrationToolBase(ABC):
    """Abstract base class for MySQL database migration operations (dump and import)."""
    def __init__(
        self,
        source: MySQLConnectionInfo,
        target: MySQLConnectionInfo,
        databases: List[str],
        skip_column_stats: bool,
        *,
        dump_tool_name: str = "mysqldump",
    ):
        self.source = source
        self.target = target
        self.databases = databases
        self.skip_column_stats = skip_column_stats
        self.dump_tool_name = dump_tool_name
        self.process_executor = ProcessExecutor()
        self._gtid: Optional[str] = None

    @abstractmethod
    def get_dump_command(self, migration_method: MySQLMigrateMethod) -> List[str]:
        """Build dump command."""

    @abstractmethod
    def get_import_command(self, migration_method: Optional[MySQLMigrateMethod] = None) -> List[str]:
        """Build import command."""

    def execute_migration(self, migration_method: MySQLMigrateMethod) -> Optional[str]:
        """
        Execute the complete migration process (dump and import).

        Args:
            migration_method: The migration method (dump or replication)

        Returns:
            GTID string for replication setup, or None for dump method
        """
        dump_cmd = self.get_dump_command(migration_method)
        import_cmd = self.get_import_command(migration_method)
        self._gtid = self.process_executor.execute_piped_commands(
            dump_cmd=dump_cmd, import_cmd=import_cmd, target=self.target, dump_tool=self.dump_tool_name
        )
        return self._gtid

    def cleanup(self) -> None:
        self.process_executor.terminate_processes()

    def get_gtid(self) -> Optional[str]:
        return self._gtid


class MySQLDumpTool(MySQLMigrationToolBase):
    """MySQL dump tool using mysqldump/mysql."""

    def get_dump_command(self, migration_method: MySQLMigrateMethod) -> List[str]:
        """Build mysqldump command."""
        # "--flush-logs" and "--master-data=2" would be good options to add, but they do not work for RDS admin
        # user - require extra permissions for `FLUSH TABLES WITH READ LOCK`
        cmd = [
            "mysqldump",
            "-h",
            self.source.hostname,
            "-P",
            str(self.source.port),
            "-u",
            self.source.username,
            f"-p{self.source.password}",
            "--compress",
            "--skip-lock-tables",
            "--single-transaction",
            "--hex-blob",
            "--routines",
            "--triggers",
            "--events",
        ]
        if migration_method == MySQLMigrateMethod.replication:
            cmd += ["--set-gtid-purged=ON"]
        else:
            cmd += ["--set-gtid-purged=OFF"]
        if self.source.ssl:
            cmd += ["--ssl-mode=REQUIRED"]
        # Dumping column statistics is not supported by MySQL < 8.0 (which is default behaviour for newer versions)
        if self.skip_column_stats:
            cmd += ["--skip-column-statistics"]
        cmd += ["--databases", "--", *[shlex.quote(db) for db in self.databases]]

        return cmd

    def get_import_command(self, migration_method: Optional[MySQLMigrateMethod] = None) -> List[str]:
        """Build mysql import command."""
        cmd = [
            "mysql", "-h", self.target.hostname, "-P",
            str(self.target.port), "-u", self.target.username, f"-p{self.target.password}", "--compress"
        ]
        if self.target.ssl:
            cmd += ["--ssl-mode=REQUIRED"]

        return cmd


class MyDumperTool(MySQLMigrationToolBase):
    """MyDumper tool using mydumper/myloader."""
    def __init__(
        self,
        source: MySQLConnectionInfo,
        target: MySQLConnectionInfo,
        databases: List[str],
        skip_column_stats: bool,
        *,
        dump_tool_name: str = "mydumper",
    ):
        super().__init__(source, target, databases, skip_column_stats, dump_tool_name=dump_tool_name)
        self.temp_dir: Optional[tempfile.TemporaryDirectory] = None
        self.temp_cnf_file: Optional[Path] = None
        self.temp_target_cnf_file: Optional[Path] = None
        self.dump_output_dir: Optional[Path] = None

    def execute_migration(self, migration_method: MySQLMigrateMethod) -> Optional[str]:
        """
        Execute the migration and extract GTID from metadata file if available.

        Args:
            migration_method: The migration method (dump or replication)

        Returns:
            GTID string for replication setup, or None
        """
        super().execute_migration(migration_method)

        # If we need GTID for replication, extract it from metadata file
        if migration_method == MySQLMigrateMethod.replication:
            gtid = self._extract_gtid_from_metadata()
            if gtid:
                self._gtid = gtid
            else:
                raise ReplicaSetupException("Failed to extract GTID from mydumper metadata for replication setup")

        return self._gtid

    def _create_temp_cnf_file(self, connection_info: MySQLConnectionInfo, filename: str = "connection.cnf") -> Path:
        """Create temporary .cnf file with credentials for secure password handling."""
        if not self.temp_dir:
            self.temp_dir = self._create_temp_directory()

        temp_cnf_path = Path(self.temp_dir.name) / filename

        with temp_cnf_path.open('w') as temp_cnf:
            temp_cnf.write(textwrap.dedent(
                f"""\
                [client]
                host={connection_info.hostname}
                port={connection_info.port}
                user={connection_info.username}
                password={connection_info.password}
                """).lstrip()
            )
            if connection_info.ssl:
                temp_cnf.write("ssl-mode=REQUIRED\n")

        # Set secure permissions
        os.chmod(temp_cnf_path, 0o600)
        return temp_cnf_path

    def _create_temp_directory(self) -> tempfile.TemporaryDirectory:
        """Create temporary directory for dump output."""
        return tempfile.TemporaryDirectory()

    def _get_dump_output_dir(self) -> Path:
        """Get or create the dump output directory (subdirectory of temp_dir)."""
        if not self.temp_dir:
            self.temp_dir = self._create_temp_directory()

        if not self.dump_output_dir:
            self.dump_output_dir = Path(self.temp_dir.name) / "dump_output"
            self.dump_output_dir.mkdir(parents=True, exist_ok=True)

        return self.dump_output_dir

    def get_dump_command(self, migration_method: MySQLMigrateMethod) -> List[str]:
        """Build mydumper command."""
        if not self.temp_cnf_file:
            self.temp_cnf_file = self._create_temp_cnf_file(self.source, "source.cnf")

        dump_output_dir = self._get_dump_output_dir()

        assert self.temp_dir is not None, "temp_dir must exist at this point"

        cmd = [
            "mydumper",
            f"--defaults-extra-file={self.temp_cnf_file}",
            "--host",
            self.source.hostname,
            "--port",
            str(self.source.port),
            "--trx-tables=0",
            "--regex",
            r"^(?!(mysql|sys|information_schema|performance_schema)\.)",
            "--compress=zstd",
            "--threads=0",
            "--triggers",
            "--events",
            "--routines",
            "--chunk-filesize=1024",
            "--sync-thread-lock-mode=FTWRL",
            "--no-backup-locks",
            "--skip-ddl-locks",
            "--checksum-all",
            "--logfile",
            f"{self.temp_dir.name}/../mydumper.log",
            "--verbose=4",
            "--stream=NO_STREAM_AND_NO_DELETE",
            "--replica-data",
            "--source-data",
            f"--outputdir={dump_output_dir}"
        ]
        if self.source.ssl:
            cmd += ["--ssl-mode=REQUIRED"]
        if self.databases:
            cmd += ["--database", ",".join(shlex.quote(db) for db in self.databases)]

        return cmd

    def get_import_command(self, migration_method: Optional[MySQLMigrateMethod] = None) -> List[str]:
        """Build myloader command."""
        if not self.temp_target_cnf_file:
            self.temp_target_cnf_file = self._create_temp_cnf_file(self.target, "target.cnf")

        dump_output_dir = self._get_dump_output_dir()

        assert self.temp_dir is not None, "temp_dir must exist at this point"

        cmd = [
            "myloader",
            f"--defaults-extra-file={self.temp_target_cnf_file}",
            "--host",
            self.target.hostname,
            "--port",
            str(self.target.port),
            "--threads=0",
            f"--directory={dump_output_dir}",
            "--optimize-keys=AFTER_IMPORT_ALL_TABLES",
            "--compress-protocol=zstd",
            "--overwrite-tables",
            "--logfile",
            f"{self.temp_dir.name}/../myloader.log",
            "--verbose=4",
            "--debug",
            "--stream=NO_STREAM_AND_NO_DELETE",
            "--drop-table",
            "--drop-database",
            "--checksum",
        ]

        return cmd

    def _check_tools_available(self) -> None:
        """Check if mydumper and myloader are available in PATH."""
        for tool in ["mydumper", "myloader"]:
            try:
                subprocess.run([tool, "--version"], capture_output=True, check=True)
            except (subprocess.CalledProcessError, FileNotFoundError) as exc:
                raise DumpToolNotFoundError(f"{tool} not found in PATH") from exc

    def _extract_gtid_from_metadata(self) -> Optional[str]:
        """Extract GTID from mydumper metadata file."""
        if not self.dump_output_dir:
            return None

        metadata_file = self.dump_output_dir / "metadata"
        if not metadata_file.exists():
            LOGGER.warning("mydumper metadata file not found")
            return None

        try:
            with metadata_file.open('r') as f:
                for line in f:
                    if line.startswith("executed_gtid_set ="):
                        gtid = line.split("=", 1)[1].strip().strip('"')
                        LOGGER.info("Extracted GTID from mydumper metadata: %s", gtid)
                        return gtid
        except (OSError, IOError) as e:
            LOGGER.warning("Failed to extract GTID from metadata: %s", e)

        return None

    def cleanup(self) -> None:
        """Cleanup temporary resources."""
        super().cleanup()

        if self.temp_dir:
            try:
                self.temp_dir.cleanup()
                self.temp_dir = None
            except (OSError, IOError) as e:
                LOGGER.warning("Failed to cleanup temporary directory: %s", e)

        self.temp_cnf_file = None
        self.temp_target_cnf_file = None


def get_dump_tool(
    tool_name: str,
    source: MySQLConnectionInfo,
    target: MySQLConnectionInfo,
    databases: List[str],
    skip_column_stats: bool,
) -> MySQLMigrationToolBase:
    """Factory function to create dump tool instances."""
    if tool_name == "mysqldump":
        return MySQLDumpTool(source, target, databases, skip_column_stats, dump_tool_name=tool_name)
    elif tool_name == "mydumper":
        return MyDumperTool(source, target, databases, skip_column_stats, dump_tool_name=tool_name)
    else:
        raise ValueError(f"Unknown dump tool: {tool_name}")
