# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate.dump_tools import (MySQLMigrationToolBase, MySQLDumpTool, MyDumperTool, get_dump_tool)
from aiven_mysql_migrate.exceptions import DumpToolNotFoundError
from aiven_mysql_migrate.utils import MySQLConnectionInfo
from aiven_mysql_migrate.enums import MySQLMigrateMethod
from pathlib import Path
from pytest import raises
from unittest.mock import patch, MagicMock
import tempfile


class TestGetDumpTool:
    """Test the factory function for creating dump tools."""
    def test_get_dump_tool_mysqldump(self):
        """Test factory returns MySQLDumpTool for mysqldump."""
        source = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
        target = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
        databases = ["testdb"]

        tool = get_dump_tool("mysqldump", source, target, databases, skip_column_stats=False)
        assert isinstance(tool, MySQLDumpTool)
        assert tool.source == source
        assert tool.target == target
        assert tool.databases == databases
        assert tool.skip_column_stats is False

    def test_get_dump_tool_mydumper(self):
        """Test factory returns MyDumperTool for mydumper."""
        source = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
        target = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
        databases = ["testdb"]

        tool = get_dump_tool("mydumper", source, target, databases, skip_column_stats=False)
        assert isinstance(tool, MyDumperTool)
        assert tool.source == source
        assert tool.target == target
        assert tool.databases == databases
        assert tool.skip_column_stats is False

    def test_get_dump_tool_unknown_tool(self):
        """Test factory raises ValueError for unknown tool."""
        source = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
        target = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
        databases = ["testdb"]

        with raises(NotImplementedError, match="Unknown dump tool: unknown_tool"):
            get_dump_tool("unknown_tool", source, target, databases, skip_column_stats=False)


class TestMySQLDumpTool:
    """Test MySQLDumpTool functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.source = MySQLConnectionInfo(  # pylint: disable=attribute-defined-outside-init
            hostname="localhost", port=3306, username="user", password="pass", ssl=True
        )
        self.target = MySQLConnectionInfo(  # pylint: disable=attribute-defined-outside-init
            hostname="localhost", port=3306, username="user", password="pass", ssl=True
        )
        self.databases = ["testdb1", "testdb2"]  # pylint: disable=attribute-defined-outside-init
        self.tool = MySQLDumpTool(  # pylint: disable=attribute-defined-outside-init
            self.source, self.target, self.databases, skip_column_stats=False
        )

    def test_get_dump_command_replication_method(self):
        """Test dump command generation for replication method."""
        cmd = self.tool.get_dump_command(MySQLMigrateMethod.replication)

        assert "mysqldump" in cmd
        assert "-h" in cmd
        assert "localhost" in cmd
        assert "-P" in cmd
        assert "3306" in cmd
        assert "-u" in cmd
        assert "user" in cmd
        assert "-ppass" in cmd
        assert "--compress" in cmd
        assert "--skip-lock-tables" in cmd
        assert "--single-transaction" in cmd
        assert "--hex-blob" in cmd
        assert "--routines" in cmd
        assert "--triggers" in cmd
        assert "--events" in cmd
        assert "--set-gtid-purged=ON" in cmd
        assert "--ssl-mode=REQUIRED" in cmd
        assert "--databases" in cmd
        assert "testdb1" in cmd
        assert "testdb2" in cmd

    def test_get_dump_command_dump_method(self):
        """Test dump command generation for dump method."""
        cmd = self.tool.get_dump_command(MySQLMigrateMethod.dump)

        assert "--set-gtid-purged=OFF" in cmd
        assert "--set-gtid-purged=ON" not in cmd

    def test_get_dump_command_ssl_disabled(self):
        """Test dump command without SSL."""
        source_no_ssl = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=False)
        tool = MySQLDumpTool(source_no_ssl, self.target, self.databases, skip_column_stats=False)
        cmd = tool.get_dump_command(MySQLMigrateMethod.replication)

        assert "--ssl-mode=REQUIRED" not in cmd

    def test_get_dump_command_skip_column_stats(self):
        """Test dump command with skip column statistics."""
        tool = MySQLDumpTool(self.source, self.target, self.databases, skip_column_stats=True)
        cmd = tool.get_dump_command(MySQLMigrateMethod.replication)

        assert "--skip-column-statistics" in cmd

    def test_get_import_command(self):
        """Test import command generation."""
        cmd = self.tool.get_import_command(MySQLMigrateMethod.replication)

        assert "mysql" in cmd
        assert "-h" in cmd
        assert "localhost" in cmd
        assert "-P" in cmd
        assert "3306" in cmd
        assert "-u" in cmd
        assert "user" in cmd
        assert "-ppass" in cmd
        assert "--compress" in cmd
        assert "--ssl-mode=REQUIRED" in cmd

    def test_get_import_command_ssl_disabled(self):
        """Test import command without SSL."""
        target_no_ssl = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=False)
        tool = MySQLDumpTool(self.source, target_no_ssl, self.databases, skip_column_stats=False)
        cmd = tool.get_import_command()

        assert "--ssl-mode=REQUIRED" not in cmd


class TestMyDumperTool:
    """Test MyDumperTool functionality."""
    def setup_method(self):
        """Set up test fixtures."""
        self.source = MySQLConnectionInfo(  # pylint: disable=attribute-defined-outside-init
            hostname="localhost", port=3306, username="user", password="pass", ssl=True
        )
        self.target = MySQLConnectionInfo(  # pylint: disable=attribute-defined-outside-init
            hostname="localhost", port=3306, username="user", password="pass", ssl=True
        )
        self.databases = ["testdb1", "testdb2"]  # pylint: disable=attribute-defined-outside-init
        self.tool = MyDumperTool(  # pylint: disable=attribute-defined-outside-init
            self.source, self.target, self.databases, skip_column_stats=False
        )

    def test_get_dump_command_replication_method(self):
        """Test mydumper command generation for replication method."""
        self.tool.setup()
        cmd = self.tool.get_dump_command(MySQLMigrateMethod.replication)

        assert "mydumper" in cmd
        assert "--trx-tables=0" in cmd
        assert any("--defaults-extra-file=" in arg for arg in cmd)
        assert "--host" in cmd
        assert "--port" in cmd
        assert "--regex" in cmd
        assert "^(?!(mysql|sys|information_schema|performance_schema)\\.)" in cmd
        assert "--compress=zstd" in cmd
        assert "--threads=0" in cmd
        assert "--triggers" in cmd
        assert "--events" in cmd
        assert "--routines" in cmd
        assert "--chunk-filesize=1024" in cmd
        assert "--sync-thread-lock-mode=FTWRL" in cmd
        assert "--no-backup-locks" in cmd
        assert "--skip-ddl-locks" in cmd
        assert "--checksum-all" in cmd
        assert "--verbose=4" in cmd
        assert "--stream=NO_STREAM_AND_NO_DELETE" in cmd
        assert "--database" == cmd[-2]  # Last argument should be output directory

    def test_get_dump_command_dump_method(self):
        """Test mydumper command generation for dump method."""
        self.tool.setup()
        cmd = self.tool.get_dump_command(MySQLMigrateMethod.dump)

        assert "mydumper" in cmd
        assert "--database" in cmd

    def test_get_import_command(self):
        """Test myloader command generation."""
        self.tool.setup()
        # First call get_dump_command to initialize temp files
        self.tool.get_dump_command(MySQLMigrateMethod.replication)
        cmd = self.tool.get_import_command(MySQLMigrateMethod.replication)

        assert "myloader" in cmd
        assert any("--defaults-extra-file=" in arg for arg in cmd)
        assert "--threads=0" in cmd
        assert any("--directory=" in arg for arg in cmd)  # Check if any argument contains --directory=
        assert "--host" in cmd
        assert "--port" in cmd
        assert "--optimize-keys=AFTER_IMPORT_ALL_TABLES" in cmd
        assert "--compress-protocol=zstd" in cmd
        assert "--verbose=4" in cmd
        assert "--overwrite-tables" in cmd
        assert "--stream=NO_STREAM" in cmd

    def test_get_import_command_dump_method(self):
        """Test myloader command generation for dump method."""
        self.tool.setup()
        # First call get_dump_command to initialize temp files
        self.tool.get_dump_command(MySQLMigrateMethod.dump)
        cmd = self.tool.get_import_command(MySQLMigrateMethod.dump)

        assert "myloader" in cmd

    def test_temp_cnf_file_creation(self):
        """Test temporary .cnf file creation and permissions."""
        self.tool.setup()
        self.tool.get_dump_command(MySQLMigrateMethod.replication)

        assert self.tool.temp_cnf_file is not None
        assert self.tool.temp_cnf_file.exists()

        # Check file permissions (should be 0600)
        stat = self.tool.temp_cnf_file.stat()
        assert oct(stat.st_mode)[-3:] == "600"

    def test_temp_cnf_file_content(self):
        """Test .cnf file contains correct credentials."""
        self.tool.setup()
        self.tool.get_dump_command(MySQLMigrateMethod.replication)

        with self.tool.temp_cnf_file.open('r', encoding='utf-8') as f:
            content = f.read()

        assert "[client]" in content
        assert f"host={self.source.hostname}" in content
        assert f"port={self.source.port}" in content
        assert f"user={self.source.username}" in content
        assert f"password={self.source.password}" in content
        assert "ssl-mode=REQUIRED" in content

    def test_temp_cnf_file_content_no_ssl(self):
        """Test .cnf file without SSL settings."""
        source_no_ssl = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=False)
        tool = MyDumperTool(source_no_ssl, self.target, self.databases, skip_column_stats=False)
        tool.setup()
        tool.get_dump_command(MySQLMigrateMethod.replication)

        with tool.temp_cnf_file.open('r', encoding='utf-8') as f:
            content = f.read()

        assert "ssl-mode=REQUIRED" not in content

    def test_temp_directory_creation(self):
        """Test temporary directory creation."""
        self.tool.setup()
        self.tool.get_dump_command(MySQLMigrateMethod.replication)

        assert self.tool.temp_dir is not None
        assert Path(self.tool.temp_dir.name).exists()

    def test_cleanup(self):
        """Test cleanup removes temporary files."""
        self.tool.setup()
        self.tool.get_dump_command(MySQLMigrateMethod.replication)

        temp_dir_path = Path(self.tool.temp_dir.name)
        temp_cnf_path = self.tool.temp_cnf_file

        # Verify files exist before cleanup
        assert temp_dir_path.exists()
        assert temp_cnf_path.exists()

        # Cleanup
        self.tool.cleanup()

        # Verify files are removed
        assert not temp_dir_path.exists()
        assert not temp_cnf_path.exists()

    def test_extract_gtid_from_metadata_success(self):
        """Test GTID extraction from metadata file."""
        # Create a temporary directory and metadata file
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create dump_output subdirectory
            dump_output_dir = Path(temp_dir) / "dump_output"
            dump_output_dir.mkdir()
            metadata_file = dump_output_dir / "metadata"
            with metadata_file.open('w') as f:
                f.write("[source]\n")
                f.write("executed_gtid_set = \"12345-67890-abcdef:1-100\"\n")
                f.write("OTHER_LINE = value\n")

            # Mock the dump_output_dir
            self.tool.dump_output_dir = dump_output_dir

            gtid = self.tool._extract_gtid_from_metadata()  # pylint: disable=protected-access
            assert gtid == "12345-67890-abcdef:1-100"

    def test_extract_gtid_from_metadata_prefers_backup(self):
        """Test GTID extraction prefers backup directory over original location."""
        # Create a temporary directory and metadata files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create dump_output subdirectory with original metadata
            dump_output_dir = Path(temp_dir) / "dump_output"
            dump_output_dir.mkdir()
            original_metadata_file = dump_output_dir / "metadata"
            with original_metadata_file.open('w') as f:
                f.write("[source]\n")
                f.write("executed_gtid_set = \"original-gtid:1-100\"\n")

            # Create backup directory with backed up metadata
            backup_dir = Path(temp_dir)
            backup_metadata_file = backup_dir / "metadata"
            with backup_metadata_file.open('w') as f:
                f.write("[source]\n")
                f.write("executed_gtid_set = \"backup-gtid:1-200\"\n")

            # Mock both directories - create a mock TemporaryDirectory-like object
            class MockTempDir:
                def __init__(self, name):
                    self.name = name

            self.tool.temp_dir = MockTempDir(temp_dir)
            self.tool.dump_output_dir = dump_output_dir

            # Should prefer backup location
            gtid = self.tool._extract_gtid_from_metadata()  # pylint: disable=protected-access
            assert gtid == "backup-gtid:1-200"

    def test_extract_gtid_from_metadata_missing_file(self):
        """Test GTID extraction when metadata file is missing."""
        # Set dump_output_dir to None to test when metadata is missing
        self.tool.dump_output_dir = None

        gtid = self.tool._extract_gtid_from_metadata()  # pylint: disable=protected-access
        assert gtid is None

    def test_extract_gtid_from_metadata_no_gtid_line(self):
        """Test GTID extraction when metadata file has no GTID line."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create dump_output subdirectory
            dump_output_dir = Path(temp_dir) / "dump_output"
            dump_output_dir.mkdir()
            metadata_file = dump_output_dir / "metadata"
            with metadata_file.open('w') as f:
                f.write("[source]\n")
                f.write("OTHER_LINE=value\n")
                f.write("ANOTHER_LINE=another_value\n")

            # Mock the dump_output_dir
            self.tool.dump_output_dir = dump_output_dir

            gtid = self.tool._extract_gtid_from_metadata()  # pylint: disable=protected-access
            assert gtid is None

    @patch('subprocess.run')
    def test_check_tools_available_success(self, mock_run):
        """Test tool availability check when tools are available."""
        mock_run.return_value.returncode = 0

        # Should not raise exception
        self.tool._check_tools_available()  # pylint: disable=protected-access

        # Should have called subprocess.run for both tools
        assert mock_run.call_count == 2
        calls = [call[0][0] for call in mock_run.call_args_list]
        assert ["mydumper", "--version"] in calls
        assert ["myloader", "--version"] in calls

    @patch('subprocess.run')
    def test_check_tools_available_mydumper_missing(self, mock_run):
        """Test tool availability check when mydumper is missing."""
        def side_effect(cmd, **kwargs):
            if cmd[0] == "mydumper":
                raise FileNotFoundError("mydumper not found")
            return MagicMock(returncode=0)

        mock_run.side_effect = side_effect

        with raises(DumpToolNotFoundError, match="mydumper not found in PATH"):
            self.tool._check_tools_available()  # pylint: disable=protected-access

    @patch('subprocess.run')
    def test_check_tools_available_myloader_missing(self, mock_run):
        """Test tool availability check when myloader is missing."""
        def side_effect(cmd, **kwargs):
            if cmd[0] == "myloader":
                raise FileNotFoundError("myloader not found")
            return MagicMock(returncode=0)

        mock_run.side_effect = side_effect

        with raises(DumpToolNotFoundError, match="myloader not found in PATH"):
            self.tool._check_tools_available()  # pylint: disable=protected-access

    def test_get_gtid_returns_none_initially(self):
        """Test get_gtid returns None before migration."""
        assert self.tool.get_gtid() is None

    def test_get_gtid_after_execution(self):
        """Test get_gtid returns GTID after successful execution."""
        # Mock the execution to set _gtid
        self.tool._gtid = "test-gtid-123"  # pylint: disable=protected-access
        assert self.tool.get_gtid() == "test-gtid-123"


class TestMySQLMigrationToolBase:
    """Test the abstract base class functionality."""
    def test_abstract_methods(self):
        """Test that MySQLMigrationToolBase cannot be instantiated directly."""
        source = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
        target = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
        databases = ["testdb"]

        # This should raise TypeError because MySQLMigrationToolBase is abstract
        with raises(TypeError, match="Can't instantiate abstract class"):
            MySQLMigrationToolBase(  # pylint: disable=abstract-class-instantiated
                source, target, databases, skip_column_stats=False
            )
