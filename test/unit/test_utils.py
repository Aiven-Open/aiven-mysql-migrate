# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate.exceptions import WrongMigrationConfigurationException
from aiven_mysql_migrate.utils import MySQLConnectionInfo, MySQLDumpProcessor, MydumperDumpProcessor
from pathlib import Path
from pytest import mark, raises
from typing import Optional, Type

import tempfile


@mark.parametrize(
    "lines,gtid", [
        (["SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '866a7051-3311-11eb-8485-0aa2f299396b:1-1213';"
          ], "866a7051-3311-11eb-8485-0aa2f299396b:1-1213"),
        ([
            "SET @@GLOBAL.GTID_PURGED= /*!80000 '+'*/'866a7051-3311-11eb-8485-0aa2f299396b:1-1213,",
            "d80acc99-4913-11eb-b1d5-42010af00042:1-249';"
        ], "866a7051-3311-11eb-8485-0aa2f299396b:1-1213,d80acc99-4913-11eb-b1d5-42010af00042:1-249"),
        ([
            "SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '866a7051-3311-11eb-8485-0aa2f299396b:1-1213,",
            "d80acc99-4913-11eb-b1d5-42010af00042:1-249,"
            "asdfcc99-4913-12eb-b1d5-42010af00042:2-321';"
        ], (
            "866a7051-3311-11eb-8485-0aa2f299396b:1-1213,"
            "d80acc99-4913-11eb-b1d5-42010af00042:1-249,"
            "asdfcc99-4913-12eb-b1d5-42010af00042:2-321"
        )),
    ]
)
def test_mysql_dump_processor_extract_gtid(lines, gtid):
    processor = MySQLDumpProcessor()
    for line in lines:
        assert processor.process_line(line) == ""

    assert processor.get_gtid() == gtid


@mark.parametrize(
    "line", [
        "SET @@SESSION.SQL_LOG_BIN= 0;", "SET @@SESSION.SQL_LOG_BIN = 0;",
        "SET @@SESSION.SQL_LOG_BIN = @MYSQLDUMP_TEMP_LOG_BIN;"
    ]
)
def test_mysql_dump_processor_remove_log_bin(line):
    helper = MySQLDumpProcessor()
    assert helper.process_line(line) == ""


@mark.parametrize(
    "line_in,line_out",
    [
        ("/*!50013 DEFINER=`admin`@`%` SQL SECURITY DEFINER */", ""),
        ("CREATE DEFINER=`admin`@`%` PROCEDURE `test`(OUT user TEXT)", "CREATE PROCEDURE `test`(OUT user TEXT)"),
        ("CREATE DEFINER=`admin`@`%` FUNCTION `test` (user CHAR(200))", "CREATE FUNCTION `test` (user CHAR(200))"),
        (
            "/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `abc` BEFORE INSERT ON `xyz` "
            "FOR EACH ROW SET @a = @a + NEW.v */;;",
            "/*!50003 CREATE*/  /*!50003 TRIGGER `abc` BEFORE INSERT ON `xyz` FOR EACH ROW SET @a = @a + NEW.v */;;"
        ),
        (
            "/*!50106 CREATE*/ /*!50117 DEFINER=`root`@`%`*/ /*!50106 EVENT `ev` ON SCHEDULE AT '2021-01-19 14:55:31' "
            "ON COMPLETION NOT PRESERVE ENABLE DO update abc.def set b=1 */ ;;",
            "/*!50106 CREATE*/  /*!50106 EVENT `ev` ON SCHEDULE AT '2021-01-19 14:55:31' ON COMPLETION NOT PRESERVE "
            "ENABLE DO update abc.def set b=1 */ ;;"
        ),
    ],
)
def test_mysql_dump_processor_remove_definers(line_in, line_out):
    helper = MySQLDumpProcessor()
    assert helper.process_line(line_in) == line_out


@mark.parametrize(
    "uri, exception_class, ssl",
    [
        ("mysql://<user>:<pwd>@<ip>:1234/", None, True),
        ("mysql://<user>:<pwd>@<ip>:1234/?", None, True),
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=DISABLED", None, False),
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=REQUIRED", None, True),
        # previously documented legacy value
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=DISABLE", None, False),
        # options with no values get dropped
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=", None, True),
        # extra parameter
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=REQUIRED&bar=baz", WrongMigrationConfigurationException, None),
        # unsupported value for ssl-mode
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=something", WrongMigrationConfigurationException, None),
        # unexpected parameter
        ("mysql://<user>:<pwd>@<ip>:1234/?foo=bar", WrongMigrationConfigurationException, None),
        # passing the ssl-mode twice
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=REQUIRED&ssl-mode=DISABLED", WrongMigrationConfigurationException, None),
        # non-numeric port
        ("mysql://<user>:<pwd>@<ip>:abcd/", WrongMigrationConfigurationException, None),
    ],
)
def test_mysql_connection_info_from_uri(uri: str, exception_class: Optional[Type[Exception]], ssl: Optional[bool]) -> None:
    if exception_class is not None:
        with raises(exception_class):
            MySQLConnectionInfo.from_uri(uri)
    else:
        assert ssl is not None
        conn_info = MySQLConnectionInfo.from_uri(uri)
        assert conn_info.ssl == ssl


@mark.parametrize(
    "uri, expected",
    [
        ("mysql://user:pwd@<ip>:1234/", "mysql://user:pwd@<ip>:1234/?ssl-mode=REQUIRED"),
        ("mysql://user:pwd@<ip>:1234/?", "mysql://user:pwd@<ip>:1234/?ssl-mode=REQUIRED"),
        ("mysql://user:pwd@<ip>:1234/?ssl-mode=DISABLED", "mysql://user:pwd@<ip>:1234/?ssl-mode=DISABLED"),
        ("mysql://user:pwd@<ip>:1234/?ssl-mode=REQUIRED", "mysql://user:pwd@<ip>:1234/?ssl-mode=REQUIRED"),
        # previously documented legacy value
        ("mysql://user:pwd@<ip>:1234/?ssl-mode=DISABLE", "mysql://user:pwd@<ip>:1234/?ssl-mode=DISABLED"),
    ],
)
def test_mysql_connection_info_to_uri(uri: str, expected: str) -> None:
    assert MySQLConnectionInfo.from_uri(uri).to_uri() == expected


@mark.parametrize(
    "password_length, template_char, exception_class",
    [
        (0, "a", WrongMigrationConfigurationException),
        (1, "a", None),
        (31, "a", None),
        (32, "a", None),
        (33, "a", WrongMigrationConfigurationException),
        (34, "a", WrongMigrationConfigurationException),
        (16, "é", None),
        (17, "é", WrongMigrationConfigurationException),
    ],
)
def test_mysql_connection_info_from_uri_password_length(
    password_length: int,
    template_char: str,
    exception_class: Optional[Type[Exception]],
) -> None:
    uri = f"mysql://<user>:{template_char * password_length}@<ip>:1234/"
    if exception_class is not None:
        with raises(exception_class):
            MySQLConnectionInfo.from_uri(uri)
    else:
        MySQLConnectionInfo.from_uri(uri)


def test_mysql_connection_info_from_uri_unquote_username_and_password() -> None:
    uri = "mysql://test%40example.com:%40%26%20%7B@<ip>:1234/?ssl-mode=DISABLED"
    conn_info = MySQLConnectionInfo.from_uri(uri)
    assert conn_info.username == "test@example.com"
    assert conn_info.password == "@& {"
    assert conn_info.to_uri() == uri


def test_mydumper_dump_processor_backs_up_metadata_file():
    """Test that MydumperDumpProcessor backs up metadata files correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dump_output_dir = Path(temp_dir) / "dump_output"
        dump_output_dir.mkdir()

        # Create a test metadata file
        metadata_file = dump_output_dir / "metadata"
        metadata_content = "[source]\nexecuted_gtid_set = \"test-gtid\"\n"
        metadata_file.write_text(metadata_content)

        processor = MydumperDumpProcessor(
            dump_output_dir=dump_output_dir,
        )

        # Process line that indicates metadata is ready
        result = processor.process_line("-- metadata 0")
        assert result == "-- metadata 0"

        # Verify file was copied
        assert processor.gtid == "test-gtid"


def test_mydumper_dump_processor_backs_up_metadata_partial_file():
    """Test that MydumperDumpProcessor backs up metadata.partial.0 files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dump_output_dir = Path(temp_dir) / "dump_output"
        dump_output_dir.mkdir()

        # Create a test metadata.partial.0 file
        metadata_partial_file = dump_output_dir / "metadata.partial.0"
        metadata_content = "partial metadata content\n"
        metadata_partial_file.write_text(metadata_content)

        processor = MydumperDumpProcessor(dump_output_dir=dump_output_dir)

        # Process line that indicates metadata.partial.0 is ready
        result = processor.process_line("-- metadata.partial.0 0")
        assert result == "-- metadata.partial.0 0"

        assert processor.gtid is None


def test_mydumper_dump_processor_handles_missing_file():
    """Test that MydumperDumpProcessor raises AssertionError for missing metadata files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dump_output_dir = Path(temp_dir) / "dump_output"
        dump_output_dir.mkdir()

        processor = MydumperDumpProcessor(dump_output_dir=dump_output_dir)

        # Process line for non-existent file - should raise AssertionError
        with raises(AssertionError, match="Metadata file not found in dump output directory"):
            processor.process_line("-- metadata 0")


def test_mydumper_dump_processor_ignores_non_metadata_lines():
    """Test that MydumperDumpProcessor ignores non-metadata lines."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dump_output_dir = Path(temp_dir) / "dump_output"
        dump_output_dir.mkdir()

        # Create a test metadata file
        metadata_file = dump_output_dir / "metadata"
        metadata_file.write_text("[source]\ntest=content\n")

        processor = MydumperDumpProcessor(dump_output_dir=dump_output_dir)
        test_lines = [
            "CREATE TABLE test (id INT);",
            "-- some comment",
            "-- metadata 0",  # This one would trigger if directories were set
        ]

        for line in test_lines:
            result = processor.process_line(line)
            assert result == line


def test_mydumper_dump_processor_creates_backup_directory():
    """Test that MydumperDumpProcessor creates backup directory if it doesn't exist."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dump_output_dir = Path(temp_dir) / "dump_output"
        dump_output_dir.mkdir()

        # Create a test metadata file
        metadata_file = dump_output_dir / "metadata"
        metadata_file.write_text("[source]\ntest=content\n")

        processor = MydumperDumpProcessor(dump_output_dir=dump_output_dir)

        # Process line - backup_dir doesn't exist yet
        result = processor.process_line("-- metadata 0")
        assert result == "-- metadata 0"


def test_mydumper_dump_processor_ignores_database_files_from_metadata_database():
    """Test that MydumperDumpProcessor ignores database files from database named 'metadata'."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dump_output_dir = Path(temp_dir) / "dump_output"
        dump_output_dir.mkdir()

        # Create test database files (these should NOT be backed up)
        database_file_sql = dump_output_dir / "metadata.table1.00000.sql.zst"
        database_file_sql.write_bytes(b"fake sql content")
        database_file_schema = dump_output_dir / "metadata.test-schema.sql.zst"
        database_file_schema.write_bytes(b"fake schema content")
        database_file_dat = dump_output_dir / "metadata.table1.dat.zst"
        database_file_dat.write_bytes(b"fake dat content")

        processor = MydumperDumpProcessor(dump_output_dir=dump_output_dir)

        # Process lines with database file names (these should be ignored)
        result1 = processor.process_line("-- metadata.table1.00000.sql.zst 0")
        assert result1 == "-- metadata.table1.00000.sql.zst 0"
        result2 = processor.process_line("-- metadata.test-schema.sql.zst 0")
        assert result2 == "-- metadata.test-schema.sql.zst 0"
        result3 = processor.process_line("-- metadata.table1.dat.zst 0")
        assert result3 == "-- metadata.table1.dat.zst 0"

        # But verify actual metadata files would still be backed up
        metadata_file = dump_output_dir / "metadata"
        metadata_content = "[source]\nexecuted_gtid_set = \"test-gtid\"\n"
        metadata_file.write_text(metadata_content)

        result4 = processor.process_line("-- metadata 0")
        assert result4 == "-- metadata 0"
        assert processor.gtid == "test-gtid"
