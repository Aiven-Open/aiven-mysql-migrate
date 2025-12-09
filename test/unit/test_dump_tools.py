# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate.dump_tools import (MySQLMigrationToolBase, MySQLDumpTool, MyDumperTool, get_dump_tool)
from aiven_mysql_migrate.utils import MySQLConnectionInfo, MydumperDumpProcessor
from aiven_mysql_migrate.enums import MySQLMigrateMethod, MySQLMigrateTool
from dataclasses import dataclass
from pathlib import Path
from pytest import raises, fixture, mark
from typing import Any, Dict, List, Type
import configparser
import stat
import tempfile


@dataclass
class FactoryTestCase:
    """Test case for dump tool factory function."""
    name: str
    tool_name: MySQLMigrateTool
    expected_class: Type[MySQLMigrationToolBase]

    def __str__(self):
        return self.name


@dataclass
class CommandTestCase:
    """Test case for complete command validation."""
    name: str
    tool_name: MySQLMigrateTool
    method: MySQLMigrateMethod
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]
    databases: List[str]
    skip_column_stats: bool
    expected_dump_command: List[str]
    expected_import_command: List[str]

    def __str__(self):
        return self.name


def _create_connection(hostname: str, ssl: bool = True) -> MySQLConnectionInfo:
    """Factory function to create database connections."""
    return MySQLConnectionInfo(hostname=hostname, port=3306, username="user", password="pass", ssl=ssl)


@fixture(name="source_connection")
def _source_connection():
    """Standard source database connection with SSL enabled."""
    return _create_connection("source", ssl=True)


@fixture(name="target_connection")
def _target_connection():
    """Standard target database connection with SSL enabled."""
    return _create_connection("target", ssl=True)


@fixture(name="databases_fixture")
def _databases_fixture():
    """Standard list of test databases."""
    return ["testdb1", "testdb2"]


# Common configurations for test cases
_SSL_CONFIG = {"hostname": "localhost", "port": 3306, "username": "user", "password": "pass", "ssl": True}
_NO_SSL_CONFIG = {"hostname": "localhost", "port": 3306, "username": "user", "password": "pass", "ssl": False}
_GTIDSET = ("866a7051-3311-11eb-8485-0aa2f299396b:1-1213,"
            "d80acc99-4913-11eb-b1d5-42010af00042:1-249,"
            "asdfcc99-4913-12eb-b1d5-42010af00042:2-321")


def _build_mysqldump_cmd(databases, gtid_mode="ON", ssl=True, skip_column_stats=False):
    """Build complete mysqldump command."""
    cmd = [
        "mysqldump",
        "-h",
        "localhost",
        "-P",
        "3306",
        "-u",
        "user",
        "-ppass",
        "--compress",
        "--skip-lock-tables",
        "--single-transaction",
        "--hex-blob",
        "--routines",
        "--triggers",
        "--events",
        f"--set-gtid-purged={gtid_mode}",
    ]
    if ssl:
        cmd.append("--ssl-mode=REQUIRED")
    if skip_column_stats:
        cmd.append("--skip-column-statistics")
    cmd.extend(["--databases", "--"] + databases)
    return cmd


def _build_mysql_cmd(ssl=True):
    """Build complete mysql import command."""
    cmd = [
        "mysql",
        "-h",
        "localhost",
        "-P",
        "3306",
        "-u",
        "user",
        "-ppass",
        "--compress",
    ]
    if ssl:
        cmd.append("--ssl-mode=REQUIRED")
    return cmd


def _build_mydumper_cmd(databases, ssl=True):
    """Build complete mydumper command with placeholder paths."""
    cmd = [
        "mydumper",
        "--defaults-extra-file=<TEMP_CNF>",
        "--host",
        "localhost",
        "--port",
        "3306",
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
        "--verbose=4",
        "--stream=NO_STREAM_AND_NO_DELETE",
        "--replica-data",
        "--source-data",
        "--outputdir=<TEMP_DIR>",
    ]
    if ssl:
        cmd.append("--ssl-mode=REQUIRED")
    if databases:
        cmd.extend(["--database", ",".join(databases)])
    return cmd


def _build_myloader_cmd():
    """Build complete myloader command with placeholder paths."""
    return [
        "myloader",
        "--defaults-extra-file=<TEMP_CNF>",
        "--host",
        "localhost",
        "--port",
        "3306",
        "--threads=0",
        "--directory=<TEMP_DIR>",
        "--optimize-keys=AFTER_IMPORT_ALL_TABLES",
        "--compress-protocol=zstd",
        "--overwrite-tables",
        "--skip-definer",
        "--verbose=4",
        "--stream=NO_STREAM",
        "--drop-table",
        "--drop-database",
        "--checksum",
    ]


def _normalize_mydumper_cmd(cmd):
    """Replace dynamic paths in mydumper commands with placeholders."""
    normalized = []
    for item in cmd:
        if item.startswith("--defaults-extra-file="):
            normalized.append("--defaults-extra-file=<TEMP_CNF>")
        elif item.startswith("--outputdir="):
            normalized.append("--outputdir=<TEMP_DIR>")
        elif item.startswith("--directory="):
            normalized.append("--directory=<TEMP_DIR>")
        else:
            normalized.append(item)
    return normalized


FACTORY_TEST_CASES = [
    FactoryTestCase(
        name="mysqldump",
        tool_name=MySQLMigrateTool.mysqldump,
        expected_class=MySQLDumpTool,
    ),
    FactoryTestCase(
        name="mydumper",
        tool_name=MySQLMigrateTool.mydumper,
        expected_class=MyDumperTool,
    ),
]

MYSQLDUMP_COMMAND_TEST_CASES = [
    CommandTestCase(
        name="mysqldump_replication_with_ssl",
        tool_name=MySQLMigrateTool.mysqldump,
        method=MySQLMigrateMethod.replication,
        source_config=_SSL_CONFIG,
        target_config=_SSL_CONFIG,
        databases=["testdb1", "testdb2"],
        skip_column_stats=False,
        expected_dump_command=_build_mysqldump_cmd(["testdb1", "testdb2"], gtid_mode="ON", ssl=True),
        expected_import_command=_build_mysql_cmd(ssl=True),
    ),
    CommandTestCase(
        name="mysqldump_dump_method_no_ssl",
        tool_name=MySQLMigrateTool.mysqldump,
        method=MySQLMigrateMethod.dump,
        source_config=_NO_SSL_CONFIG,
        target_config=_NO_SSL_CONFIG,
        databases=["testdb1", "testdb2"],
        skip_column_stats=False,
        expected_dump_command=_build_mysqldump_cmd(["testdb1", "testdb2"], gtid_mode="OFF", ssl=False),
        expected_import_command=_build_mysql_cmd(ssl=False),
    ),
    CommandTestCase(
        name="mysqldump_with_skip_column_stats",
        tool_name=MySQLMigrateTool.mysqldump,
        method=MySQLMigrateMethod.replication,
        source_config=_SSL_CONFIG,
        target_config=_SSL_CONFIG,
        databases=["testdb1", "testdb2"],
        skip_column_stats=True,
        expected_dump_command=_build_mysqldump_cmd(["testdb1", "testdb2"], gtid_mode="ON", ssl=True, skip_column_stats=True),
        expected_import_command=_build_mysql_cmd(ssl=True),
    ),
]

MYDUMPER_COMMAND_TEST_CASES = [
    CommandTestCase(
        name="mydumper_replication_with_ssl",
        tool_name=MySQLMigrateTool.mydumper,
        method=MySQLMigrateMethod.replication,
        source_config=_SSL_CONFIG,
        target_config=_SSL_CONFIG,
        databases=["testdb1", "testdb2"],
        skip_column_stats=False,
        expected_dump_command=_build_mydumper_cmd(["testdb1", "testdb2"], ssl=True),
        expected_import_command=_build_myloader_cmd(),
    ),
    CommandTestCase(
        name="mydumper_dump_method_no_ssl",
        tool_name=MySQLMigrateTool.mydumper,
        method=MySQLMigrateMethod.dump,
        source_config=_NO_SSL_CONFIG,
        target_config=_NO_SSL_CONFIG,
        databases=["testdb"],
        skip_column_stats=False,
        expected_dump_command=_build_mydumper_cmd(["testdb"], ssl=False),
        expected_import_command=_build_myloader_cmd(),
    ),
]


class TestGetDumpTool:
    """Test the factory function for creating dump tools."""

    @mark.parametrize("test_case", FACTORY_TEST_CASES, ids=str)
    def test_get_dump_tool_returns_correct_type(self, test_case: FactoryTestCase, source_connection, target_connection):
        """Test factory returns correct tool type based on tool name."""
        databases = ["testdb"]

        tool = get_dump_tool(test_case.tool_name, source_connection, target_connection, databases, skip_column_stats=False)

        assert isinstance(tool, test_case.expected_class)
        assert tool.source == source_connection
        assert tool.target == target_connection
        assert tool.databases == databases
        assert tool.skip_column_stats is False

    def test_get_dump_tool_unknown_tool(self, source_connection, target_connection):
        """Test factory raises NotImplementedError for unknown tool."""
        databases = ["testdb"]

        with raises(NotImplementedError, match="Unknown dump tool: unknown_tool"):
            get_dump_tool("unknown_tool", source_connection, target_connection, databases, skip_column_stats=False)


class TestMySQLDumpTool:
    """Test MySQLDumpTool functionality with complete command validation."""

    @mark.parametrize("test_case", MYSQLDUMP_COMMAND_TEST_CASES, ids=str)
    def test_complete_command_generation(self, test_case: CommandTestCase):
        """Test complete dump and import command generation."""
        source = MySQLConnectionInfo(**test_case.source_config)
        target = MySQLConnectionInfo(**test_case.target_config)

        tool = MySQLDumpTool(source, target, test_case.databases, skip_column_stats=test_case.skip_column_stats)

        actual_dump_cmd = tool.get_dump_command(test_case.method)
        assert actual_dump_cmd == test_case.expected_dump_command

        actual_import_cmd = tool.get_import_command(test_case.method)
        assert actual_import_cmd == test_case.expected_import_command


class TestMyDumperTool:
    """Test MyDumperTool functionality."""

    @mark.parametrize("test_case", MYDUMPER_COMMAND_TEST_CASES, ids=str)
    def test_complete_command_generation(self, test_case: CommandTestCase):
        """Test complete dump and import command generation with normalized paths."""
        source = MySQLConnectionInfo(**test_case.source_config)
        target = MySQLConnectionInfo(**test_case.target_config)

        tool = MyDumperTool(source, target, test_case.databases, skip_column_stats=test_case.skip_column_stats)
        tool.setup()

        try:
            dump_cmd = tool.get_dump_command(test_case.method)
            import_cmd = tool.get_import_command(test_case.method)

            # Normalize dynamic paths before comparison
            normalized_dump = _normalize_mydumper_cmd(dump_cmd)
            normalized_import = _normalize_mydumper_cmd(import_cmd)

            assert normalized_dump == test_case.expected_dump_command
            assert normalized_import == test_case.expected_import_command
        finally:
            tool.cleanup()

    @mark.parametrize(
        "ssl_enabled", [True, False],
        ids=["with_ssl", "without_ssl"]
    )
    def test_temp_files_and_content(
        self, ssl_enabled, *, target_connection, databases_fixture
    ):
        """Test temporary file creation, permissions, and content."""
        source = _create_connection("source", ssl=ssl_enabled)
        tool = MyDumperTool(source, target_connection, databases_fixture, skip_column_stats=False)
        tool.setup()

        # Verify temp files exist
        assert tool.temp_cnf_file is not None
        assert tool.temp_cnf_file.exists()
        assert tool.temp_dir_path is not None
        assert tool.temp_dir_path.exists()

        # Check file permissions (should be 0600)
        assert stat.S_IMODE(tool.temp_cnf_file.stat().st_mode) == 0o600

        config = configparser.ConfigParser()
        config.read(tool.temp_cnf_file)

        # Verify [client] section exists
        assert "client" in config.sections()
        assert len(config.sections()) == 1, "File should contain only [client] section"

        # Verify exact key-value pairs
        client_section = config["client"]
        assert client_section["host"] == source.hostname
        assert client_section["port"] == str(source.port)
        assert client_section["user"] == source.username
        assert client_section["password"] == source.password

        if ssl_enabled:
            assert "ssl-mode" in client_section
            assert client_section["ssl-mode"] == "REQUIRED"
        else:
            assert "ssl-mode" not in client_section

        # Verify no unexpected keys exist
        expected_keys = {"host", "port", "user", "password"}
        if ssl_enabled:
            expected_keys.add("ssl-mode")
        assert set(client_section.keys()) == expected_keys

        tool.cleanup()

    def test_cleanup(self, source_connection, target_connection, databases_fixture):
        """Test cleanup removes temporary files."""
        tool = MyDumperTool(source_connection, target_connection, databases_fixture, skip_column_stats=False)
        tool.setup()

        temp_dir_path = tool.temp_dir_path
        temp_cnf_path = tool.temp_cnf_file

        # Verify files exist before cleanup
        assert temp_dir_path.exists()
        assert temp_cnf_path.exists()

        # Cleanup
        tool.cleanup()

        # Verify files are removed
        assert not temp_dir_path.exists()
        assert not temp_cnf_path.exists()

    def test_extract_gtid_from_metadata(self):
        """Test GTID extraction."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dump_output_dir = Path(temp_dir)
            metadata_file = dump_output_dir / "metadata"
            with metadata_file.open('w') as f:
                f.write("[source]\n")
                f.write(f"executed_gtid_set = \"{_GTIDSET}\"\n")

            processor = MydumperDumpProcessor(dump_output_dir=dump_output_dir)
            processor.save_gtid_from_metadata()
            assert processor.gtid == _GTIDSET

    def test_extract_gtid_from_metadata_no_gtid_line(self):
        """Test GTID extraction when metadata file has no GTID line."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dump_output_dir = Path(temp_dir) / "dump_output"
            dump_output_dir.mkdir()
            metadata_file = dump_output_dir / "metadata"
            with metadata_file.open('w') as f:
                f.write("[source]\n")
                f.write("OTHER_LINE=value\n")
                f.write("ANOTHER_LINE=another_value\n")

            processor = MydumperDumpProcessor(dump_output_dir=dump_output_dir)

            gtid = processor._extract_gtid_from_metadata()  # pylint: disable=protected-access
            assert gtid is None

    def test_get_gtid_returns_none_initially(self, source_connection, target_connection, databases_fixture):
        """Test get_gtid returns None before migration."""
        tool = MyDumperTool(source_connection, target_connection, databases_fixture, skip_column_stats=False)
        assert tool.get_gtid() is None

    def test_get_gtid_after_execution(self, source_connection, target_connection, databases_fixture):
        """Test get_gtid returns GTID after successful execution."""
        tool = MyDumperTool(source_connection, target_connection, databases_fixture, skip_column_stats=False)
        tool._gtid = _GTIDSET  # pylint: disable=protected-access
        assert tool.get_gtid() == _GTIDSET


class TestMySQLMigrationToolBase:
    """Test the abstract base class functionality."""

    def test_abstract_methods(self, source_connection, target_connection):
        """Test that MySQLMigrationToolBase cannot be instantiated directly."""
        databases = ["testdb"]

        # This should raise TypeError because MySQLMigrationToolBase is abstract
        with raises(TypeError, match="Can't instantiate abstract class"):
            MySQLMigrationToolBase(  # pylint: disable=abstract-class-instantiated
                source_connection, target_connection, databases, skip_column_stats=False
            )
