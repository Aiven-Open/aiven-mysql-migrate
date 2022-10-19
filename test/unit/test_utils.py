from typing import Optional, Type

from aiven_mysql_migrate.exceptions import WrongMigrationConfigurationException
from aiven_mysql_migrate.utils import MySQLDumpProcessor, MySQLConnectionInfo
from pytest import mark, raises


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
    [("/*!50013 DEFINER=`admin`@`%` SQL SECURITY DEFINER */", ""),
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
     )]
)
def test_mysql_dump_processor_remove_definers(line_in, line_out):
    helper = MySQLDumpProcessor()
    assert helper.process_line(line_in) == line_out


@mark.parametrize(
    "uri, exception_class",
    [
        ("mysql://<user>:<pwd>@<ip>:1234/", None),
        ("mysql://<user>:<pwd>@<ip>:1234/?", None),
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=DISABLE", None),
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=REQUIRE", None),
        # options with no values get dropped
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=", None),
        # extra parameter
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=REQUIRE&bar=baz", WrongMigrationConfigurationException),
        # unsupported value for ssl-mode
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=something", WrongMigrationConfigurationException),
        # unexpected parameter
        ("mysql://<user>:<pwd>@<ip>:1234/?foo=bar", WrongMigrationConfigurationException),
        # passing the ssl-mode twice
        ("mysql://<user>:<pwd>@<ip>:1234/?ssl-mode=REQUIRE&ssl-mode=DISABLE", WrongMigrationConfigurationException),
        # non-numeric port
        ("mysql://<user>:<pwd>@<ip>:abcd/", WrongMigrationConfigurationException),
    ],
)
def test_mysql_connection_info_from_uri(uri: str, exception_class: Optional[Type[Exception]]) -> None:
    if exception_class is not None:
        with raises(exception_class):
            MySQLConnectionInfo.from_uri(uri)
    else:
        MySQLConnectionInfo.from_uri(uri)
