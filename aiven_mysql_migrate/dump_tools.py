# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from abc import ABC, abstractmethod
from aiven_mysql_migrate.migration_executor import ProcessExecutor
from aiven_mysql_migrate.utils import MySQLConnectionInfo
from enum import Enum
from typing import List, Optional

import logging
import shlex

LOGGER = logging.getLogger(__name__)


class MySQLMigrateMethod(str, Enum):
    dump = "dump"
    replication = "replication"


class DumpToolBase(ABC):
    """Abstract base class for database dump tools."""
    def __init__(
        self,
        source: MySQLConnectionInfo,
        target: MySQLConnectionInfo,
        databases: List[str],
        skip_column_stats: bool,
    ):
        self.source = source
        self.target = target
        self.databases = databases
        self.skip_column_stats = skip_column_stats
        self.process_executor = ProcessExecutor()
        self._gtid: Optional[str] = None

    @abstractmethod
    def get_dump_command(self, migration_method: MySQLMigrateMethod) -> List[str]:
        """Build dump command."""

    @abstractmethod
    def get_import_command(self) -> List[str]:
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
        import_cmd = self.get_import_command()

        try:
            _, _, gtid = self.process_executor.execute_pipe_commands(
                dump_cmd=dump_cmd, import_cmd=import_cmd, target=self.target
            )
            self._gtid = gtid
            return self._gtid
        except Exception as e:
            LOGGER.error("Error during migration: %s", e)
            self.cleanup()
            raise

    def cleanup(self) -> None:
        """Cleanup any temporary resources."""
        self.process_executor.terminate_processes()

    def get_gtid(self) -> Optional[str]:
        """Return the extracted GTID for replication setup."""
        return self._gtid


class MySQLDumpTool(DumpToolBase):
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

    def get_import_command(self) -> List[str]:
        """Build mysql import command."""
        cmd = [
            "mysql", "-h", self.target.hostname, "-P",
            str(self.target.port), "-u", self.target.username, f"-p{self.target.password}", "--compress"
        ]
        if self.target.ssl:
            cmd += ["--ssl-mode=REQUIRED"]

        return cmd
