# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate import config
from aiven_mysql_migrate.dump_tools import MySQLMigrationToolBase, get_dump_tool
from aiven_mysql_migrate.enums import MySQLMigrateTool, MySQLMigrateMethod
from aiven_mysql_migrate.exceptions import (
    DatabaseTooLargeException, EndpointConnectionException, GTIDModeDisabledException, MissingReplicationGrants,
    NothingToMigrateException, ReplicaSetupException, ReplicationNotAvailableException, ServerIdsOverlappingException,
    SSLNotSupportedException, TooManyDatabasesException, UnsupportedBinLogFormatException, UnsupportedMySQLEngineException,
    UnsupportedMySQLVersionException, WrongMigrationConfigurationException
)
from aiven_mysql_migrate.utils import MySQLConnectionInfo, PrivilegeCheckUser, select_global_var
from looseversion import LooseVersion
from pathlib import Path
from pymysql.constants.ER import HANDSHAKE_ERROR
from typing import List, Optional

import json
import logging
import os
import pymysql
import signal
import time

LOGGER = logging.getLogger(__name__)


class MySQLMigration:
    source: MySQLConnectionInfo
    target: MySQLConnectionInfo
    target_master: MySQLConnectionInfo | None

    _databases: Optional[List[str]] = None

    def __init__(
        self,
        *,
        source_uri: str,
        target_uri: str,
        target_master_uri: Optional[str],
        filter_dbs: Optional[str] = None,
        privilege_check_user: Optional[str] = None,
        output_meta_file: Optional[Path] = None,
        dump_tool: MySQLMigrateTool = MySQLMigrateTool.mysqldump,
    ):
        self.dump_tool_name = dump_tool
        self.dump_tool: Optional[MySQLMigrationToolBase] = None

        self.source = MySQLConnectionInfo.from_uri(source_uri, name="source")
        self.target = MySQLConnectionInfo.from_uri(target_uri, name="target")
        self.target_master = MySQLConnectionInfo.from_uri(
            target_master_uri, name="target master"
        ) if target_master_uri else None

        self.ignore_dbs = config.IGNORE_SYSTEM_DATABASES.copy()
        if filter_dbs:
            self.ignore_dbs.update({db.strip() for db in filter_dbs.split(",")})

        self.skip_column_stats = False

        self.privilege_check_user = None
        if privilege_check_user:
            self.privilege_check_user = PrivilegeCheckUser.parse(privilege_check_user)
        self.output_meta_file = output_meta_file

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._stop_migration)
        signal.signal(signal.SIGTERM, self._stop_migration)
        signal.signal(signal.SIGPIPE, self._stop_migration)

    def _stop_migration(self, signum, frame):
        LOGGER.info("Received signal: %s", signum)
        if self.dump_tool:
            self.dump_tool.cleanup()

    def list_databases(self) -> List[str]:
        with self.source.cur() as cur:
            cur.execute(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                f"WHERE SCHEMA_NAME NOT IN ({', '.join(['%s'] * len(self.ignore_dbs))})", tuple(self.ignore_dbs)
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
                f"WHERE TABLE_SCHEMA IN ({','.join(['%s'] * len(self.databases))}) "
                "AND ENGINE IS NOT NULL AND UPPER(ENGINE) != 'INNODB'", tuple(self.databases)
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
                cur.execute("SHOW MASTER STATUS")
                master_status = cur.fetchone()
                if master_status is None:
                    raise GTIDModeDisabledException(
                        f"GTID mode should be enabled on the {conn_info.name}: SHOW MASTER STATUS is empty"
                    )
                executed_gtid_set = master_status.get("Executed_Gtid_Set", None)
                if not executed_gtid_set:
                    raise GTIDModeDisabledException(
                        f"GTID mode should be enabled on the {conn_info.name}: Executed_Gtid_Set is empty"
                    )

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
                if e.args[0] == HANDSHAKE_ERROR:
                    raise SSLNotSupportedException(f"SSL is required, but not supported by the {conn_info.name}") from e
                raise EndpointConnectionException(f"Connection to {conn_info.name} failed: {e}") from e

    def _check_databases_count(self):
        LOGGER.info("Checking for databases count limit")

        db_count = len(self.databases)
        if db_count == 0:
            raise NothingToMigrateException("No databases to migrate")
        elif db_count > config.MYSQL_MAX_DATABASES:
            raise TooManyDatabasesException(
                f"Too many databases to migrate: {len(self.databases)} (> {config.MYSQL_MAX_DATABASES})"
            )

    def _check_database_size(self, max_size: float):
        LOGGER.info("Checking max total databases size")

        with self.source.cur() as cur:
            cur.execute(
                "SELECT SUM(DATA_LENGTH + INDEX_LENGTH) AS size FROM INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA NOT IN ({', '.join(['%s'] * len(self.ignore_dbs))})", tuple(self.ignore_dbs)
            )
            source_size = cur.fetchone()["size"] or 0
        if source_size > max_size:
            raise DatabaseTooLargeException()

    def _check_bin_log_format(self):
        with self.source.cur() as cur:
            row_format = select_global_var(cur, "binlog_format")
            if row_format.upper() != "ROW":
                raise UnsupportedBinLogFormatException(f"Unsupported binary log format: {row_format}, only ROW is supported")

    def run_checks(
        self,
        force_method: Optional[MySQLMigrateMethod] = None,
        dbs_max_total_size: Optional[float] = None
    ) -> MySQLMigrateMethod:
        """Raises an exception if one of the the pre-checks fails, otherwise a method to be used for migration.
        If force_method is set, re-raises validation exceptions in case the chosen method is not possible."""
        migration_method = MySQLMigrateMethod.replication if force_method is None else force_method
        fallback_to_dump_method = force_method is None

        if force_method is not None:
            LOGGER.info("Forcing migration method %r", migration_method)

        if migration_method == MySQLMigrateMethod.replication and not self.target_master:
            if not fallback_to_dump_method:
                raise WrongMigrationConfigurationException("TARGET_MASTER_SERVICE_URI is not set")

            LOGGER.warning(
                "Replication method is not available due to missing TARGET_MASTER_SERVICE_URI, falling back to dump"
            )
            migration_method = MySQLMigrateMethod.dump

        self._check_connections()
        self._check_databases_count()
        if dbs_max_total_size is not None:
            self._check_database_size(max_size=dbs_max_total_size)

        if self.source.version < LooseVersion("8.0.0") or self.target.version < LooseVersion("8.0.0"):
            self.skip_column_stats = True
        if migration_method == MySQLMigrateMethod.dump:
            return migration_method

        # Check if replication is possible
        try:
            # Version check should be always first, there is no sense
            # in other checks until we are sure that we are dealing
            # with supported ones.
            self._check_versions_replication_support()
            self._check_user_can_replicate()
            self._check_gtid_mode_enabled()
            self._check_engine_support()
            self._check_server_id_overlapping()
            self._check_bin_log_format()
        except ReplicationNotAvailableException as e:
            if not fallback_to_dump_method:
                raise

            LOGGER.warning("Replication is not possible. Falling back to dump method, details: %s", e)
            migration_method = MySQLMigrateMethod.dump

        return migration_method

    def _stop_and_reset_slave(self):
        LOGGER.info("Stopping replication on target database")

        with self.target_master.cur() as cur:
            cur.execute("STOP SLAVE")
            cur.execute("RESET SLAVE ALL")

    def _stop_replication(self):
        LOGGER.info("Stopping replication")

        self._stop_and_reset_slave()

    def _migrate_data(self, migration_method: MySQLMigrateMethod) -> Optional[str]:
        """Migrate data using the configured dump tool, return GTID from the dump"""
        self.dump_tool = get_dump_tool(
            self.dump_tool_name,
            self.source,
            self.target,
            self.databases,
            self.skip_column_stats
        )

        return self.dump_tool.execute_migration(migration_method)

    def _set_gtid(self, gtid: str):
        LOGGER.info("GTID from the dump is `%s`", gtid)
        assert self.target_master is not None
        with self.target_master.cur() as cur:
            # Check which of the source GTIDs are not yet applied - needed in case of running migration again on top
            # of finished one
            cur.execute("SELECT GTID_SUBTRACT(%s, @@GLOBAL.GTID_EXECUTED) AS DIFF", (gtid, ))
            new_gtids = cur.fetchone()["DIFF"]
            if not new_gtids:
                LOGGER.info("GTID_EXECUTED already contains GTID set from the dump, skipping `SET @@GTID_PURGED` step")
                return

            LOGGER.info("Adding new GTID set on the target service `%s`", new_gtids)
            cur.execute("SET @@GLOBAL.GTID_PURGED = %s", ("+" + new_gtids, ))
            cur.execute("COMMIT")

    def _start_replication(self):
        LOGGER.info("Setting up replication %s -> %s", self.source.hostname, self.target.hostname)

        with self.target_master.cur() as cur:
            query = (
                "CHANGE MASTER TO MASTER_HOST = %s, MASTER_PORT = %s, MASTER_USER = %s, MASTER_PASSWORD = %s, "
                f"MASTER_AUTO_POSITION = 1, MASTER_SSL = {1 if self.source.ssl else 0}, "
                "MASTER_SSL_VERIFY_SERVER_CERT = 0, MASTER_SSL_CA = '', MASTER_SSL_CAPATH = ''"
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
                f"CHANGE REPLICATION FILTER REPLICATE_WILD_IGNORE_TABLE = ({', '.join('%s' for _ in self.ignore_dbs)})",
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

    def start(self, *,
              migration_method: MySQLMigrateMethod, seconds_behind_master: int, stop_replication: bool = False) -> None:
        LOGGER.info("Start migration of the following databases:")
        for db in self.databases:
            LOGGER.info("\t%s", db)

        if self.output_meta_file:
            assert os.access(self.output_meta_file.parent, os.W_OK), f"Meta file {self.output_meta_file} is not writable"
            self.output_meta_file.unlink(missing_ok=True)  # type: ignore

        gtid = self._migrate_data(migration_method)
        LOGGER.info("Migration of dump data has finished, GTID value from the dump: `%s`", gtid)

        if self.output_meta_file:
            with self.output_meta_file.open("w") as meta_file:
                meta_file.write(json.dumps({"dump_gtids": gtid if gtid else ""}))
                meta_file.close()

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
