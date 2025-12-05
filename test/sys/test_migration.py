from aiven_mysql_migrate.config import IGNORE_SYSTEM_DATABASES
from aiven_mysql_migrate.enums import MySQLMigrateTool, MySQLMigrateMethod
from aiven_mysql_migrate.exceptions import (
    DatabaseTooLargeException, ReplicationNotAvailableException, SSLNotSupportedException
)
from aiven_mysql_migrate.migration import MySQLMigration
from aiven_mysql_migrate.utils import MySQLConnectionInfo
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from pytest import fixture, mark

import json
import logging
import pytest
import random
import string
import time

MYSQL_WAIT_RETRIES = 30
MYSQL_WAIT_SLEEP = 2
LOGGER = logging.getLogger(__name__)


class TimeoutException(Exception):
    pass


@fixture(name="db_name")
def random_db_name():
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))


def my_wait(host, ssl=True, retries=MYSQL_WAIT_RETRIES) -> MySQLConnectionInfo:
    uri = f"mysql://root:test@{host}/"
    if not ssl:
        uri += "?ssl-mode=DISABLED"
    conn = MySQLConnectionInfo.from_uri(uri)
    for _ in range(retries):
        try:
            with conn.cur() as cur:
                cur.execute("SELECT VERSION()")
                return conn
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.warning("%s is not yet ready: %s", host, ex)
            time.sleep(MYSQL_WAIT_SLEEP)
    raise TimeoutException(f"Timeout while waiting for {host}")


@mark.parametrize(
    "src,dst,dump_tool", [
        (my_wait("mysql57-src-1"), my_wait("mysql80-dst-1"), "mysqldump"),
        (my_wait("mysql80-src-2"), my_wait("mysql80-dst-2"), "mysqldump"),
        (my_wait("mysql80-src-2"), my_wait("mysql80-dst-2"), "mydumper"),

    ]
)
def test_migration_replication(
    src: MySQLConnectionInfo, dst: MySQLConnectionInfo, dump_tool: str, db_name: str, tmp_path: Path
) -> None:
    output_meta_file = tmp_path / "meta.json"
    with dst.cur() as cur:
        cur.execute("STOP REPLICA FOR CHANNEL ''")
    with src.cur() as cur:
        cur.execute(f"CREATE DATABASE `{db_name}`")
        cur.execute(f"USE `{db_name}`")
        cur.execute("CREATE TABLE test (ID TEXT)")
        cur.execute("INSERT INTO test (ID) VALUES (%s)", ["test_data"])
        cur.execute("COMMIT")
        cur.execute("SELECT @@GLOBAL.SERVER_UUID AS UUID")
        server_uuid = cur.fetchone()["UUID"]

    migration = MySQLMigration(
        source_uri=src.to_uri(),
        target_uri=dst.to_uri(),
        target_master_uri=dst.to_uri(),
        privilege_check_user="root@%",
        output_meta_file=output_meta_file,
        dump_tool=MySQLMigrateTool(dump_tool),
    )
    method = migration.run_checks()
    assert method == MySQLMigrateMethod.replication
    migration.start(migration_method=method, seconds_behind_master=0)
    assert output_meta_file.exists()
    with output_meta_file.open("r") as meta_file:
        meta = json.loads(meta_file.read())
    assert "dump_gtids" in meta
    assert server_uuid in meta["dump_gtids"]

    with dst.cur() as cur:
        cur.execute(f"SELECT ID FROM {db_name}.test")
        res = cur.fetchall()
        assert len(res) == 1 and res[0]["ID"] == "test_data"

    with src.cur() as cur:
        cur.execute(f"INSERT INTO {db_name}.test (ID) VALUES (%s)", ["repl_data"])
        cur.execute("COMMIT")

    for _ in range(5):
        with dst.cur() as cur:
            cur.execute(f"SELECT ID FROM {db_name}.test")
            res = cur.fetchall()
            if len(res) == 2 and sorted(["test_data", "repl_data"]) == sorted([item["ID"] for item in res]):
                return
        time.sleep(1)

    raise TimeoutException()


@mark.parametrize(
    "src,dst,dump_tool", [
        (my_wait("mysql57-src-1"), my_wait("mysql80-dst-1"), "mysqldump"),
        (my_wait("mysql80-src-2"), my_wait("mysql80-dst-2"), "mysqldump"),
        (my_wait("mysql80-src-2"), my_wait("mysql80-dst-2"), "mydumper"),

    ]
)
def test_migration_replication_with_reestablish_replication(
        src: MySQLConnectionInfo, dst: MySQLConnectionInfo, dump_tool: str, db_name: str, tmp_path: Path
) -> None:
    output_meta_file = tmp_path / "meta.json"
    with dst.cur() as cur:
        cur.execute("STOP REPLICA FOR CHANNEL ''")
    with src.cur() as cur:
        cur.execute(f"CREATE DATABASE `{db_name}`")
        cur.execute(f"USE `{db_name}`")
        cur.execute("CREATE TABLE test (ID TEXT)")
        cur.execute("INSERT INTO test (ID) VALUES (%s)", ["test_data"])
        cur.execute("COMMIT")
        cur.execute("SELECT @@GLOBAL.SERVER_UUID AS UUID")
        server_uuid = cur.fetchone()["UUID"]

    migration = MySQLMigration(
        source_uri=src.to_uri(),
        target_uri=dst.to_uri(),
        target_master_uri=dst.to_uri(),
        privilege_check_user="root@%",
        output_meta_file=output_meta_file,
        dump_tool=MySQLMigrateTool(dump_tool),
    )
    method = migration.run_checks()
    assert method == MySQLMigrateMethod.replication
    migration.start(migration_method=method, seconds_behind_master=0)
    assert output_meta_file.exists()
    with output_meta_file.open("r") as meta_file:
        meta = json.loads(meta_file.read())
    assert "dump_gtids" in meta
    assert server_uuid in meta["dump_gtids"]

    with dst.cur() as cur:
        cur.execute(f"SELECT ID FROM {db_name}.test")
        res = cur.fetchall()
        assert len(res) == 1 and res[0]["ID"] == "test_data"

    with dst.cur() as cur:
        cur.execute("STOP REPLICA FOR CHANNEL ''")

    with src.cur() as cur:
        cur.execute(f"INSERT INTO {db_name}.test (ID) VALUES (%s)", ["repl_data"])
        cur.execute("COMMIT")

    migration = MySQLMigration(
        source_uri=src.to_uri(),
        target_uri=dst.to_uri(),
        target_master_uri=dst.to_uri(),
        privilege_check_user="root@%",
        output_meta_file=output_meta_file,
        dump_tool=MySQLMigrateTool(dump_tool),
    )
    method = migration.run_checks()
    assert method == MySQLMigrateMethod.replication
    migration.start(migration_method=method, seconds_behind_master=0, reestablish_replication=True)

    for _ in range(5):
        with dst.cur() as cur:
            cur.execute(f"SELECT ID FROM {db_name}.test")
            res = cur.fetchall()
            if len(res) == 2 and sorted(["test_data", "repl_data"]) == sorted([item["ID"] for item in res]):
                return
        time.sleep(1)

    raise TimeoutException()


@mark.parametrize(
    "src,dst,dump_tool", [
        (my_wait("mysql80-src-3"), my_wait("mysql80-dst-3"), "mysqldump"),
        (my_wait("mysql80-src-3"), my_wait("mysql80-dst-3"), "mydumper"),
        (my_wait("mysql57-src-invalid-gtid"), my_wait("mysql80-dst-1"), "mysqldump"),
    ]
)
def test_migration_fallback(src: MySQLConnectionInfo, dst: MySQLConnectionInfo, dump_tool: str, db_name: str) -> None:
    with src.cur() as cur:
        cur.execute(f"CREATE DATABASE `{db_name}`")
        cur.execute(f"USE `{db_name}`")
        cur.execute("CREATE TABLE test (ID TEXT)")
        cur.execute("INSERT INTO test (ID) VALUES (%s)", ["test_data"])
        cur.execute("CREATE PROCEDURE test_proc (OUT body TEXT) BEGIN SELECT 'test_body'; END")
        cur.execute("COMMIT")

    migration = MySQLMigration(
        source_uri=src.to_uri(),
        target_uri=dst.to_uri(),
        target_master_uri=dst.to_uri(),
        dump_tool=MySQLMigrateTool(dump_tool),
    )
    method = migration.run_checks()
    assert method == MySQLMigrateMethod.dump
    migration.start(migration_method=method, seconds_behind_master=0)

    with dst.cur() as cur:
        cur.execute(f"SELECT ID FROM {db_name}.test")
        res = cur.fetchall()
        assert len(res) == 1 and res[0]["ID"] == "test_data"

        cur.execute(f"call {db_name}.test_proc(@body)")
        res = cur.fetchall()
        assert len(res) == 1 and res[0]["test_body"] == "test_body"


@mark.parametrize(
    "src,dst,forced_method,context,dump_tool", [
        (my_wait("mysql80-src-2"), my_wait("mysql80-dst-2"), MySQLMigrateMethod.replication, does_not_raise(), "mysqldump"),
        (my_wait("mysql80-src-2"), my_wait("mysql80-dst-2"), MySQLMigrateMethod.replication, does_not_raise(), "mydumper"),
        (
            my_wait("mysql80-src-3"), my_wait("mysql80-dst-3"), MySQLMigrateMethod.replication,
            pytest.raises(ReplicationNotAvailableException), "mysqldump"
        ),
        (
            my_wait("mysql80-src-3"), my_wait("mysql80-dst-3"), MySQLMigrateMethod.replication,
            pytest.raises(ReplicationNotAvailableException), "mydumper"
        ),
        (my_wait("mysql80-src-3"), my_wait("mysql80-dst-3"), MySQLMigrateMethod.dump, does_not_raise(), "mysqldump"),
        (my_wait("mysql80-src-3"), my_wait("mysql80-dst-3"), MySQLMigrateMethod.dump, does_not_raise(), "mydumper"),
    ]
)
def test_force_migration_method(  # pylint: disable=too-many-positional-arguments
    src, dst, forced_method, context, dump_tool, db_name
):
    with src.cur() as cur:
        cur.execute(f"CREATE DATABASE `{db_name}`")
        cur.execute(f"USE `{db_name}`")
        cur.execute("CREATE TABLE test (ID TEXT)")
        cur.execute("INSERT INTO test (ID) VALUES (%s)", ["test_data"])
        cur.execute("COMMIT")

    migration = MySQLMigration(
        source_uri=src.to_uri(),
        target_uri=dst.to_uri(),
        target_master_uri=dst.to_uri(),
        privilege_check_user="root@%",
        dump_tool=MySQLMigrateTool(dump_tool),
    )

    with context:
        method = migration.run_checks(force_method=forced_method)
        assert method == forced_method


@mark.parametrize(
    "src,dst,dump_tool", [
        (my_wait("mysql80-src-3"), my_wait("mysql80-dst-3"), "mysqldump"),
        (my_wait("mysql80-src-3"), my_wait("mysql80-dst-3"), "mydumper"),
    ]
)
def test_database_size_check(src, dst, dump_tool, db_name):
    ignore_dbs = IGNORE_SYSTEM_DATABASES.copy()
    ignore_dbs.add(db_name)

    with src.cur() as cur:
        cur.execute(
            "SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA NOT IN ({', '.join(['%s'] * len(ignore_dbs))})", tuple(ignore_dbs)
        )
        other_test_dbs = {table_schema["TABLE_SCHEMA"] for table_schema in cur.fetchall()}

    with src.cur() as cur:
        cur.execute(f"CREATE DATABASE `{db_name}`")
        cur.execute(f"USE `{db_name}`")
        cur.execute("CREATE TABLE test (ID TEXT)")
        cur.execute("INSERT INTO test (ID) VALUES (%s)", ["test_data"])

    migration = MySQLMigration(
        source_uri=src.to_uri(),
        target_uri=dst.to_uri(),
        target_master_uri=dst.to_uri(),
        dump_tool=MySQLMigrateTool(dump_tool),
    )

    # Should fit to this size.
    migration.run_checks(dbs_max_total_size=1048576)

    # For this the database is too large.
    with pytest.raises(DatabaseTooLargeException):
        migration.run_checks(dbs_max_total_size=0)

    migration.ignore_dbs.add(db_name)
    # Other tests have added dbs so we ignore them too.
    for db in other_test_dbs:
        migration.ignore_dbs.add(db)
    # This is ok if we ignore all DBs there is.
    migration.run_checks(dbs_max_total_size=0)


@mark.parametrize(
    "src,dst,dump_tool", [
        (my_wait("mysql80-src-4", ssl=False), my_wait("mysql80-dst-3"), "mysqldump"),
        (my_wait("mysql80-src-4", ssl=False), my_wait("mysql80-dst-3"), "mydumper"),
    ]
)
def test_database_ssl_disabled(src, dst, dump_tool, db_name):
    ignore_dbs = IGNORE_SYSTEM_DATABASES.copy()
    ignore_dbs.add(db_name)

    with src.cur() as cur:
        cur.execute(f"CREATE DATABASE `{db_name}`")
        cur.execute(f"USE `{db_name}`")
        cur.execute("CREATE TABLE test (ID TEXT)")

    # Default check without SSL should pass
    migration = MySQLMigration(
        source_uri=src.to_uri(),
        target_uri=dst.to_uri(),
        target_master_uri=dst.to_uri(),
        dump_tool=MySQLMigrateTool(dump_tool),
    )
    migration.run_checks()

    # Enable SSL and now it should fail
    src.ssl = True
    migration = MySQLMigration(
        source_uri=src.to_uri(),
        target_uri=dst.to_uri(),
        target_master_uri=dst.to_uri(),
        dump_tool=MySQLMigrateTool(dump_tool),
    )
    with pytest.raises(SSLNotSupportedException):
        migration.run_checks()
