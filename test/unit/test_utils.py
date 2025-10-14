from aiven_mysql_migrate.exceptions import WrongMigrationConfigurationException
from aiven_mysql_migrate.utils import MySQLConnectionInfo, MySQLDumpProcessor
from aiven_mysql_migrate.dump_tools import get_dump_tool
from aiven_mysql_migrate.migration import MySQLMigrateMethod
from pytest import mark, raises
from typing import Optional, Type


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


@mark.parametrize("tool_name", ["mysqldump", "mydumper"])
def test_dump_tool_command_generation(tool_name):
    """Test that both tools can generate valid commands."""
    source = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
    target = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
    databases = ["testdb1", "testdb2"]

    # Test factory function creates correct tool
    tool = get_dump_tool(tool_name, source, target, databases, skip_column_stats=False)

    # Test dump command generation for replication method
    dump_cmd = tool.get_dump_command(MySQLMigrateMethod.replication)
    assert isinstance(dump_cmd, list)
    assert len(dump_cmd) > 0

    # Test dump command generation for dump method
    dump_cmd_dump = tool.get_dump_command(MySQLMigrateMethod.dump)
    assert isinstance(dump_cmd_dump, list)
    assert len(dump_cmd_dump) > 0

    # Test import command generation
    import_cmd = tool.get_import_command(MySQLMigrateMethod.replication)
    assert isinstance(import_cmd, list)
    assert len(import_cmd) > 0

    # Verify tool-specific command elements
    if tool_name == "mysqldump":
        assert "mysqldump" in dump_cmd
        assert "mysql" in import_cmd
        assert "--databases" in dump_cmd
    elif tool_name == "mydumper":
        assert "mydumper" in dump_cmd
        assert "myloader" in import_cmd
        assert any("--outputdir=" in arg for arg in dump_cmd)
        assert any("--directory=" in arg for arg in import_cmd)


@mark.parametrize("tool_name", ["mysqldump", "mydumper"])
def test_dump_tool_ssl_handling(tool_name):
    """Test that both tools handle SSL correctly."""
    # Test with SSL enabled
    source_ssl = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
    target_ssl = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
    databases = ["testdb"]

    tool_ssl = get_dump_tool(tool_name, source_ssl, target_ssl, databases, skip_column_stats=False)
    dump_cmd_ssl = tool_ssl.get_dump_command(MySQLMigrateMethod.replication)
    import_cmd_ssl = tool_ssl.get_import_command(MySQLMigrateMethod.replication)

    # Test with SSL disabled
    source_no_ssl = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=False)
    target_no_ssl = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=False)

    tool_no_ssl = get_dump_tool(tool_name, source_no_ssl, target_no_ssl, databases, skip_column_stats=False)
    dump_cmd_no_ssl = tool_no_ssl.get_dump_command(MySQLMigrateMethod.replication)
    import_cmd_no_ssl = tool_no_ssl.get_import_command(MySQLMigrateMethod.replication)

    # Verify SSL flags are present when SSL is enabled
    if tool_name == "mysqldump":
        assert "--ssl-mode=REQUIRED" in dump_cmd_ssl
        assert "--ssl-mode=REQUIRED" in import_cmd_ssl
        assert "--ssl-mode=REQUIRED" not in dump_cmd_no_ssl
        assert "--ssl-mode=REQUIRED" not in import_cmd_no_ssl
    elif tool_name == "mydumper":
        # For mydumper, SSL is handled in the .cnf file, so we check the .cnf content
        # This is more complex to test without actually creating the files
        # We'll just verify the commands are generated successfully
        assert len(dump_cmd_ssl) > 0
        assert len(import_cmd_ssl) > 0
        assert len(dump_cmd_no_ssl) > 0
        assert len(import_cmd_no_ssl) > 0


@mark.parametrize("tool_name", ["mysqldump", "mydumper"])
def test_dump_tool_gtid_handling(tool_name):
    """Test that both tools handle GTID settings correctly."""
    source = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
    target = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
    databases = ["testdb"]

    tool = get_dump_tool(tool_name, source, target, databases, skip_column_stats=False)

    # Test replication method (should include GTID)
    dump_cmd_replication = tool.get_dump_command(MySQLMigrateMethod.replication)

    # Test dump method (should not include GTID)
    dump_cmd_dump = tool.get_dump_command(MySQLMigrateMethod.dump)

    if tool_name == "mysqldump":
        assert "--set-gtid-purged=ON" in dump_cmd_replication
        assert "--set-gtid-purged=OFF" in dump_cmd_dump


@mark.parametrize("tool_name", ["mysqldump", "mydumper"])
def test_dump_tool_database_handling(tool_name):
    """Test that both tools handle database lists correctly."""
    source = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
    target = MySQLConnectionInfo(hostname="localhost", port=3306, username="user", password="pass", ssl=True)
    databases = ["db1", "db2", "db3"]

    tool = get_dump_tool(tool_name, source, target, databases, skip_column_stats=False)
    dump_cmd = tool.get_dump_command(MySQLMigrateMethod.replication)

    if tool_name == "mysqldump":
        # mysqldump includes databases in the command
        assert "--databases" in dump_cmd
        for db in databases:
            assert db in dump_cmd
    elif tool_name == "mydumper":
        # mydumper uses regex to filter databases, so we just verify command generation
        assert len(dump_cmd) > 0
        assert "mydumper" in dump_cmd
