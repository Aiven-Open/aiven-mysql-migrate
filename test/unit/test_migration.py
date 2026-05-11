# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate.enums import MySQLMigrateMethod
from aiven_mysql_migrate.migration import MySQLMigration
from aiven_mysql_migrate.utils import MySQLConnectionInfo
from contextlib import contextmanager
from unittest.mock import MagicMock

import logging
import pymysql
import pytest


def _make_migration(databases, target_cursor):
    """Construct a MySQLMigration with mocked source/target connections."""
    migration = MySQLMigration.__new__(MySQLMigration)
    migration._databases = databases  # pylint: disable=protected-access

    target = MagicMock(spec=MySQLConnectionInfo)

    @contextmanager
    def _cur(**_kwargs):
        yield target_cursor

    target.cur.side_effect = _cur
    migration.target = target
    return migration


def test_analyze_tables_runs_per_table():
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        # First fetchall: list of tables on the target.
        [
            {"TABLE_SCHEMA": "db1", "TABLE_NAME": "t1"},
            {"TABLE_SCHEMA": "db1", "TABLE_NAME": "t2"},
            {"TABLE_SCHEMA": "db2", "TABLE_NAME": "t3"},
        ],
        # Subsequent fetchall calls drain ANALYZE TABLE result rows.
        [], [], [],
    ]
    migration = _make_migration(["db1", "db2"], cursor)

    migration._analyze_tables()  # pylint: disable=protected-access

    executed = [call.args[0] for call in cursor.execute.call_args_list]
    # First call enumerates tables.
    assert executed[0].startswith("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")
    assert "TABLE_TYPE = 'BASE TABLE'" in executed[0]
    assert "UPPER(ENGINE) = 'INNODB'" in executed[0]
    # Followed by one ANALYZE per table, in enumeration order, with NO_WRITE_TO_BINLOG.
    assert executed[1:] == [
        "ANALYZE NO_WRITE_TO_BINLOG TABLE `db1`.`t1`",
        "ANALYZE NO_WRITE_TO_BINLOG TABLE `db1`.`t2`",
        "ANALYZE NO_WRITE_TO_BINLOG TABLE `db2`.`t3`",
    ]


def test_analyze_tables_continues_on_per_table_error(caplog):
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [
            {"TABLE_SCHEMA": "db1", "TABLE_NAME": "t1"},
            {"TABLE_SCHEMA": "db1", "TABLE_NAME": "t2"},
            {"TABLE_SCHEMA": "db1", "TABLE_NAME": "t3"},
        ],
        [],  # t1 ANALYZE result rows
        [],  # t3 ANALYZE result rows (t2 raised before fetchall)
    ]

    def _execute(stmt, *_args, **_kwargs):
        if stmt == "ANALYZE NO_WRITE_TO_BINLOG TABLE `db1`.`t2`":
            raise pymysql.Error("boom")

    cursor.execute.side_effect = _execute
    migration = _make_migration(["db1"], cursor)

    with caplog.at_level(logging.WARNING):
        migration._analyze_tables()  # pylint: disable=protected-access

    executed = [call.args[0] for call in cursor.execute.call_args_list]
    # All three ANALYZE attempts were made despite the failure of t2.
    assert "ANALYZE NO_WRITE_TO_BINLOG TABLE `db1`.`t1`" in executed
    assert "ANALYZE NO_WRITE_TO_BINLOG TABLE `db1`.`t2`" in executed
    assert "ANALYZE NO_WRITE_TO_BINLOG TABLE `db1`.`t3`" in executed
    messages = [rec.getMessage() for rec in caplog.records]
    assert any("`db1`.`t2`" in msg and "boom" in msg for msg in messages)


def test_analyze_tables_escapes_backticks_in_identifiers():
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [{"TABLE_SCHEMA": "weird`db", "TABLE_NAME": "tbl`name"}],
        [],
    ]
    migration = _make_migration(["weird`db"], cursor)

    migration._analyze_tables()  # pylint: disable=protected-access

    executed = [call.args[0] for call in cursor.execute.call_args_list]
    assert executed[1] == "ANALYZE NO_WRITE_TO_BINLOG TABLE `weird``db`.`tbl``name`"


def test_analyze_tables_no_databases_short_circuits():
    cursor = MagicMock()
    migration = _make_migration([], cursor)

    migration._analyze_tables()  # pylint: disable=protected-access

    cursor.execute.assert_not_called()


@pytest.mark.parametrize("skip_flag", [True, False])
def test_start_migration_respects_skip_flag(skip_flag, monkeypatch):
    migration = MySQLMigration.__new__(MySQLMigration)
    migration._databases = ["db1"]  # pylint: disable=protected-access
    migration.output_meta_file = None

    calls = []
    monkeypatch.setattr(migration, "_migrate_data", lambda *_a, **_kw: None)
    monkeypatch.setattr(migration, "_analyze_tables", lambda: calls.append("analyze"))

    migration.start_migration(
        migration_method=MySQLMigrateMethod.dump,
        seconds_behind_master=-1,
        skip_analyze_after_import=skip_flag,
    )
    assert calls == ([] if skip_flag else ["analyze"])


def test_start_migration_skips_analyze_when_reestablishing(monkeypatch):
    migration = MySQLMigration.__new__(MySQLMigration)
    migration._databases = ["db1"]  # pylint: disable=protected-access
    migration.output_meta_file = None

    calls = []
    monkeypatch.setattr(migration, "_migrate_data", lambda *_a, **_kw: None)
    monkeypatch.setattr(migration, "_analyze_tables", lambda: calls.append("analyze"))
    # Replication path under reestablish skips dump and replication setup;
    # we only care that analyze does not run.
    monkeypatch.setattr(migration, "_set_gtid", lambda *_a, **_kw: None)
    monkeypatch.setattr(migration, "_start_replication", lambda: None)
    monkeypatch.setattr(migration, "_ensure_target_replica_running", lambda *_a, **_kw: None)

    migration.start_migration(
        migration_method=MySQLMigrateMethod.replication,
        seconds_behind_master=-1,
        reestablish_replication=True,
    )
    assert not calls
