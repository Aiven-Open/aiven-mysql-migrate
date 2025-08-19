from aiven_mysql_migrate.exceptions import WrongMigrationConfigurationException
from aiven_mysql_migrate.utils import MySQLConnectionInfo, MySQLDumpProcessor
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
