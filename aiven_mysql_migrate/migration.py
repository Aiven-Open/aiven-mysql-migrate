# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate import config
from aiven_mysql_migrate.exceptions import (
    EndpointConnectionException, GTIDModeDisabledException, MissingReplicationGrants, MySQLDumpException,
    MySQLImportException, NothingToMigrateException, ReplicaSetupException, ReplicationNotAvailableException,
    ServerIdsOverlappingException, TooManyDatabasesException, UnsupportedBinLogFormatException,
    UnsupportedMySQLEngineException, UnsupportedMySQLVersionException
)
from aiven_mysql_migrate.utils import MySQLConnectionInfo, MySQLDumpProcessor, PrivilegeCheckUser, select_global_var
from concurrent import futures
from dataclasses import dataclass
from distutils.version import LooseVersion
from subprocess import Popen
from typing import List, Optional

import concurrent
import enum
import logging
import pymysql
import shlex
import signal
import subprocess
import sys
import time

LOGGER = logging.getLogger(__name__)


@enum.unique
class MySQLMigrateMethod(str, enum.Enum):
    dump = "dump"
    replication = "replication"


@dataclass
class MySQLMigrateMethodValidation:
    method: MySQLMigrateMethod
    status: str


class MySQLMigration:
    source: MySQLConnectionInfo
    target: MySQLConnectionInfo
    target_master: MySQLConnectionInfo

    _databases: Optional[List[str]] = None

    def __init__(
        self,
        *,
        source_uri: str,
        target_uri: str,
        target_master_uri: Optional[str],
        filter_dbs: Optional[str] = None,
        privilege_check_user: Optional[str] = None
    ):
        self.mysqldump_proc: Optional[Popen] = None
        self.mysql_proc: Optional[Popen] = None

        self.source = MySQLConnectionInfo.from_uri(source_uri, name="source")
        self.target = MySQLConnectionInfo.from_uri(target_uri, name="target")
        self.target_master = MySQLConnectionInfo.from_uri(
            target_master_uri, name="target master"
        ) if target_master_uri else None

        self.ignore_dbs = config.IGNORE_SYSTEM_DATABASES
        if filter_dbs:
            self.ignore_dbs.update({db.strip() for db in filter_dbs.split(",")})

        self.skip_column_stats = False

        self.privilege_check_user = None
        if privilege_check_user:
            self.privilege_check_user = PrivilegeCheckUser.parse(privilege_check_user)

        signal.signal(signal.SIGINT, self._stop_migration)
        signal.signal(signal.SIGTERM, self._stop_migration)
        signal.signal(signal.SIGPIPE, self._stop_migration)

    def _stop_migration(self, signum, frame):
        LOGGER.info("Received signal: %s", signum)
        for subproc in (self.mysqldump_proc, self.mysql_proc):
            if subproc:
                LOGGER.warning("Terminating subprocess with pid: %s", subproc.pid)
                subproc.kill()

    def list_databases(self) -> List[str]:
        with self.source.cur() as cur:
            cur.execute(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ({format_dbs})".format(
                    format_dbs=", ".join(["%s"] * len(self.ignore_dbs))
                ), tuple(self.ignore_dbs)
            )
            return [row["SCHEMA_NAME"] for row in cur.fetchall()]

    @property
    def databases(self) -> List[str]:
        if self._databases is None:
            self._databases = self.list_databases()
        return self._databases

    def _check_versions_replication_support(self):
        """Supported versions for replication method: 8.X -> 8.X, 5.7.X -> 8.X"""
        LOGGER.info("Checking MySQL versions for replication support")

        if (
            LooseVersion("5.7.0") <= LooseVersion(self.source.version) < LooseVersion("8.1")
            and LooseVersion("8.0.0") <= LooseVersion(self.target.version) < LooseVersion("8.1")
        ):
            LOGGER.info("\tSource - %s, target - %s -- OK", self.source.version, self.target.version)
        else:
            raise UnsupportedMySQLVersionException(
                f"Replication method is not supported between MySQL versions: source - {self.source.version},"
                f"target - {self.target.version}"
            )

    def _check_server_id_overlapping(self):
        LOGGER.info("Checking for server id overlap")

        with self.source.cur() as source_cur, self.target.cur() as target_cur:
            source_server_id = select_global_var(source_cur, "server_id")
            target_server_id = select_global_var(target_cur, "server_id")

        if source_server_id == target_server_id:
            raise ServerIdsOverlappingException(
                "Replication method is not available due to server_id overlapping,"
                f"source and target have the same value - {source_server_id}"
            )

    def _check_engine_support(self):
        LOGGER.info("Checking for source engine support")

        with self.source.cur() as cur:
            cur.execute(
                "SELECT COUNT(DISTINCT(ENGINE)) AS count FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA IN ({format_dbs}) AND ENGINE IS NOT NULL AND UPPER(ENGINE) != 'INNODB'".format(
                    format_dbs=",".join(["%s"] * len(self.databases))
                ), tuple(self.databases)
            )
            res = cur.fetchone()
            if not res["count"] == 0:
                raise UnsupportedMySQLEngineException("Only InnoDB engine is supported")

    def _check_gtid_mode_enabled(self):
        for conn_info in (self.source, self.target):
            LOGGER.info("Checking if GTID mode is enabled on the %s", conn_info.name)
            with conn_info.cur() as cur:
                gtid_mode = select_global_var(cur, "gtid_mode")
                if gtid_mode.upper() != "ON":
                    raise GTIDModeDisabledException(f"GTID mode should be enabled on the {conn_info.name}")

    def _check_user_can_replicate(self):
        LOGGER.info("Checking if user has replication grants on the source")

        user_can_replicate = any(grant in self.source.global_grants for grant in ("REPLICATION SLAVE", "ALL PRIVILEGES"))
        if not user_can_replicate:
            raise MissingReplicationGrants("User does not have replication permissions")

    def _check_connections(self):
        LOGGER.info("Checking connections to service URIs")

        conn_infos = [self.source, self.target]
        if self.target_master:
            conn_infos.append(self.target_master)

        for conn_info in conn_infos:
            try:
                with conn_info.cur():
                    pass
            except pymysql.Error as e:
                raise EndpointConnectionException(f"Connection to {conn_info.name} failed") from e

    def _check_databases_count(self):
        LOGGER.info("Checking for databases count limit")

        db_count = len(self.databases)
        if db_count == 0:
            raise NothingToMigrateException()
        elif db_count > config.MYSQL_MAX_DATABASES:
            raise TooManyDatabasesException(
                f"Too many databases to migrate: {len(self.databases)} (> {config.MYSQL_MAX_DATABASES})"
            )

    def _check_bin_log_format(self):
        with self.source.cur() as cur:
            row_format = select_global_var(cur, "binlog_format")
            if row_format.upper() != "ROW":
                raise UnsupportedBinLogFormatException(f"Unsupported binary log format: {row_format}, only ROW is supported")

    def run_checks(self) -> MySQLMigrateMethodValidation:
        """Raises an exception if one of the the pre-checks fails, otherwise a method to be used for migration"""
        migration_method = MySQLMigrateMethod.replication

        msg: Optional[str] = "No error"
        if not self.target_master:
            msg = "Replication method is not available due to missing TARGET_MASTER_SERVICE_URI, falling back to dump"
            LOGGER.warning(msg)
            migration_method = MySQLMigrateMethod.dump

        self._check_connections()
        self._check_databases_count()

        if self.source.version < LooseVersion("8.0.0") or self.target.version < LooseVersion("8.0.0"):
            self.skip_column_stats = True

        if migration_method == MySQLMigrateMethod.dump:
            return MySQLMigrateMethodValidation(status=msg, method=migration_method)

        # Check if replication is possible
        try:
            self._check_user_can_replicate()
            self._check_gtid_mode_enabled()
            self._check_versions_replication_support()
            self._check_engine_support()
            self._check_server_id_overlapping()
            self._check_bin_log_format()
        except ReplicationNotAvailableException as e:
            msg = f"Replication is not possible. Falling back to dump method, details: {e}"
            LOGGER.warning(msg)
            migration_method = MySQLMigrateMethod.dump

        return MySQLMigrateMethodValidation(status=msg, method=migration_method)

    def _stop_and_reset_slave(self):
        LOGGER.info("Stopping replication on target database")

        with self.target_master.cur() as cur:
            cur.execute("STOP SLAVE")
            cur.execute("RESET SLAVE ALL")

    def _stop_replication(self):
        LOGGER.info("Stopping replication")

        self._stop_and_reset_slave()

    def _get_dump_command(self, migration_method: MySQLMigrateMethod) -> List[str]:
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

    def _get_import_command(self) -> List[str]:
        cmd = [
            "mysql", "-h", self.target.hostname, "-P",
            str(self.target.port), "-u", self.target.username, f"-p{self.target.password}", "--compress"
        ]
        if self.target.ssl:
            cmd += ["--ssl-mode=REQUIRED"]

        return cmd

    def _migrate_data(self, migration_method: MySQLMigrateMethod) -> Optional[str]:
        """Migrate data using mysqldump/mysql cli into the target database, return GTID from the dump"""
        LOGGER.info("Starting import MySQL dump file into target database")

        dump_processor = MySQLDumpProcessor()
        self.mysqldump_proc = Popen(
            self._get_dump_command(migration_method=migration_method),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        self.mysql_proc = Popen(self._get_import_command(), stdin=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # make mypy happy
        assert self.mysqldump_proc.stdout
        assert self.mysqldump_proc.stderr
        assert self.mysql_proc.stdin

        # If sql_require_primary_key is ON globally - it's not possible to import tables without a primary key
        with self.target.cur() as cur:
            if select_global_var(cur, "sql_require_primary_key") == 1:
                self.mysql_proc.stdin.write("SET SESSION sql_require_primary_key = 0;")

        def _reader_stdout():
            for line in self.mysqldump_proc.stdout:
                line = dump_processor.process_line(line.rstrip())

                if not line:
                    continue

                LOGGER.debug("dump: %s", line)
                self.mysql_proc.stdin.write(line + "\n")

            self.mysql_proc.stdin.flush()
            self.mysql_proc.stdin.close()

        def _reader_stderr(proc):
            for line in proc.stderr:
                sys.stderr.write(line)

        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            for future in concurrent.futures.as_completed([
                executor.submit(_reader_stdout),
                executor.submit(_reader_stderr, self.mysqldump_proc),
                executor.submit(_reader_stderr, self.mysql_proc)
            ]):
                future.result()

        export_code = self.mysqldump_proc.wait()
        import_code = self.mysql_proc.wait()

        if export_code != 0:
            raise MySQLDumpException(f"Error while importing data from the source database, exit code: {export_code}")

        if import_code != 0:
            raise MySQLImportException(f"Error while importing data into the target database, exit code: {import_code}")

        return dump_processor.get_gtid()

    def _set_gtid(self, gtid: str):
        LOGGER.info("Setting GTID in target database to `%s`", gtid)

        with self.target_master.cur() as cur:
            cur.execute("SET @@GLOBAL.GTID_PURGED = %s", (gtid, ))
            cur.execute("COMMIT")

    def _start_replication(self):
        LOGGER.info("Setting up replication %s -> %s", self.source.hostname, self.target.hostname)

        with self.target_master.cur() as cur:
            query = (
                "CHANGE MASTER TO MASTER_HOST = %s, MASTER_PORT = %s, MASTER_USER = %s, MASTER_PASSWORD = %s, "
                "MASTER_AUTO_POSITION = 1, MASTER_SSL = 1"
            )
            if LooseVersion(self.target.version) >= LooseVersion("8.0.19"):
                query += ", REQUIRE_ROW_FORMAT = 1"
            if LooseVersion(self.target.version) >= LooseVersion("8.0.20"):
                query += ", REQUIRE_TABLE_PRIMARY_KEY_CHECK = OFF"

            query_params = [self.source.hostname, self.source.port, self.source.username, self.source.password]

            if self.privilege_check_user:
                query += f", PRIVILEGE_CHECKS_USER = {self.privilege_check_user.sql_format}"
                query_params += self.privilege_check_user.sql_params

            cur.execute(query, query_params)
            cur.execute(
                # For some reason REPLICATE_IGNORE_DB does not work if SQL statements are executed within the context of
                # other database, e.g. when doing:
                # > USE some_database;
                # > CREATE USER alice@'%' IDENTIFIED BY RANDOM PASSWORD;
                # This will lead to mysql.user not being filtered out and user alice to be replicated into replica,
                # even though it's in the list of databases to ignore. However REPLICATE_WILD_IGNORE_TABLE works
                # properly, so it's possible to specify `mysql.%` to be ignored and in this case it does not matter
                # from which context statement is executed.
                "CHANGE REPLICATION FILTER REPLICATE_WILD_IGNORE_TABLE = ({format_ignore_tables})".format(
                    format_ignore_tables=", ".join("%s" for _ in self.ignore_dbs)
                ),
                [f"{db}.%" for db in self.ignore_dbs]
            )
            cur.execute("START SLAVE")

    def _ensure_target_replica_running(self, check_interval: float = 2.0, retries: int = 30):
        LOGGER.info("Ensure replica is running")

        with self.target.cur() as cur:
            for _ in range(retries):
                cur.execute("SHOW SLAVE STATUS")
                rows = cur.fetchall()
                if not rows:
                    raise ReplicaSetupException()

                try:
                    slave_status = next(
                        row for row in rows
                        if row["Master_Host"] == self.source.hostname and row["Master_Port"] == self.source.port
                    )
                except StopIteration as e:
                    raise ReplicaSetupException() from e

                if slave_status["Slave_IO_Running"] == "Yes" and slave_status["Slave_SQL_Running"] == "Yes":
                    return

                time.sleep(check_interval)

            raise ReplicaSetupException()

    def _wait_for_replication(self, *, seconds_behind_master: int = 0, check_interval: float = 2.0):
        LOGGER.info("Wait for replication to catch up")

        while True:
            with self.target.cur() as cur:
                cur.execute("SHOW SLAVE STATUS")
                rows = cur.fetchall()
                if not rows:
                    raise ReplicaSetupException()

                try:
                    slave_status = next(
                        row for row in rows
                        if row["Master_Host"] == self.source.hostname and row["Master_Port"] == self.source.port
                    )
                except StopIteration as e:
                    raise ReplicaSetupException() from e

                lag = slave_status["Seconds_Behind_Master"]
                if lag is None:
                    raise ReplicaSetupException()

                LOGGER.info("Current replication lag: %s seconds", lag)
                if lag <= seconds_behind_master:
                    return

            time.sleep(check_interval)

    def start(self, *, migration_method: MySQLMigrateMethod, seconds_behind_master: int, stop_replication: bool = False):
        LOGGER.info("Start migration of the following databases:")
        for db in self.databases:
            LOGGER.info("\t%s", db)

        gtid = self._migrate_data(migration_method)
        LOGGER.info("Migration of dump data has finished, GTID value from the dump: `%s`", gtid)

        if migration_method == MySQLMigrateMethod.replication:
            LOGGER.info("Setting up replication to the target DB")

            assert gtid, "GTID should be set"
            self._set_gtid(gtid)
            self._start_replication()
            self._ensure_target_replica_running()

            if seconds_behind_master > -1:
                self._wait_for_replication(seconds_behind_master=seconds_behind_master)

            if stop_replication:
                self._stop_replication()
