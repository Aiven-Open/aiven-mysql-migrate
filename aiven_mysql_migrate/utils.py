# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate import config
from aiven_mysql_migrate.exceptions import WrongMigrationConfigurationException
from dataclasses import dataclass
from typing import AnyStr, Dict, List, Optional
from urllib.parse import parse_qs, quote, unquote, urlparse

import contextlib
import pymysql
import re

DEFAULT_MYSQL_PORT = 3306
ALLOWED_OPTIONS = {"ssl-mode"}
# See https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_change_master_password_length
MAX_PASSWORD_LENGTH = 32

ROUTINE_DEFINER_RE = re.compile("^CREATE DEFINER *= *(`.*?`@`.*?`) +(.*$)")
IMPORT_DEFINER_RE = re.compile(r"^/\*!50013 DEFINER *= *`.*?`@`.*?` +SQL SECURITY DEFINER \*/$")
EXTRA_DEFINER_RE = re.compile(r"^(/\*!(?:50003|50106) CREATE *\*/ *)(/\*!(?:50017|50117) +DEFINER *= *`.*?`@`.*?`\*/)(.*$)")

GTID_START_RE = re.compile(r"^SET +@@GLOBAL.GTID_PURGED *= */\*!80000 +'\+'\*/ *'([^']*)")
GTID_END_RE = re.compile(r"^(.*?)' *;")

LOG_BIN_RE = re.compile(r"^SET +@@SESSION.SQL_LOG_BIN *= *.*?;$")

GLOBAL_GRANTS_RE = re.compile("^GRANT +(.*) +ON +\\*\\.\\* +TO.*$")


@dataclass
class MySQLConnectionInfo:
    hostname: str
    port: int
    username: str
    password: str
    ssl: Optional[bool] = True

    name: Optional[str] = None

    _version: Optional[str] = None
    _global_grants: Optional[List[str]] = None

    @staticmethod
    def from_uri(uri: str, name: Optional[str] = None):
        try:
            res = urlparse(uri, scheme="mysql")
            if res.scheme != "mysql" or not res.username or not res.password or not res.hostname:
                raise WrongMigrationConfigurationException(f"{uri!r} is not a valid service URI")

        except ValueError as e:
            raise WrongMigrationConfigurationException(f"{uri!r} is not a valid service URI") from e

        try:
            port = res.port or DEFAULT_MYSQL_PORT
        except ValueError as e:
            raise WrongMigrationConfigurationException(f"{uri!r} invalid port") from e

        password = unquote(res.password)
        if len(password.encode()) > MAX_PASSWORD_LENGTH:
            raise WrongMigrationConfigurationException("The password for the replication user must not exceed 32 characters")

        options = parse_qs(res.query)
        MySQLConnectionInfo._validate_options(options)

        ssl = not (options and options.get("ssl-mode", ["DISABLED"]) in (["DISABLE"], ["DISABLED"]))
        return MySQLConnectionInfo(
            hostname=res.hostname,
            port=port,
            username=unquote(res.username),
            password=password,
            ssl=ssl,
            name=name,
        )

    @staticmethod
    def _validate_options(options: Dict[str, List[AnyStr]]) -> None:
        if not ALLOWED_OPTIONS.issuperset(options):
            raise WrongMigrationConfigurationException(f"Only {', '.join(ALLOWED_OPTIONS)} allowed as uri parameter")

        if options and "ssl-mode" in options:
            ssl_mode = options["ssl-mode"]
            if ssl_mode not in (["DISABLE"], ["DISABLED"], ["REQUIRED"]):
                # don't include the legacy value in the error message
                raise WrongMigrationConfigurationException("ssl-mode must be either 'DISABLED' or 'REQUIRED'")

    def to_uri(self):
        ssl_mode = "DISABLED" if not self.ssl else "REQUIRED"
        return f"mysql://{quote(self.username)}:{quote(self.password)}@{self.hostname}:{self.port}/?ssl-mode={ssl_mode}"

    def repr(self):
        return self.name

    def _connect(self):
        ssl = None
        if self.ssl:
            ssl = {"require": True}
        return pymysql.connect(
            charset="utf8mb4",
            connect_timeout=config.MYSQL_CONNECTION_TIMEOUT,
            cursorclass=pymysql.cursors.DictCursor,
            host=self.hostname,
            password=self.password,
            read_timeout=config.MYSQL_READ_TIMEOUT,
            port=self.port,
            ssl=ssl,
            user=self.username,
            write_timeout=config.MYSQL_WRITE_TIMEOUT,
        )

    @property
    def version(self) -> str:
        if self._version is None:
            with self.cur() as source_cur:
                self._version = select_global_var(source_cur, "version")
        return self._version

    @property
    def global_grants(self) -> List[str]:
        if self._global_grants is None:
            self._global_grants = []
            with self.cur() as cur:
                cur.execute("SHOW GRANTS FOR CURRENT_USER")
                rows = cur.fetchall()
                for row in rows:
                    match = GLOBAL_GRANTS_RE.match(next(iter(row.values())))
                    if match:
                        self._global_grants += [grant.strip().upper() for grant in match.group(1).split(",")]
        return self._global_grants

    @contextlib.contextmanager
    def ctx(self):
        context = None
        try:
            context = self._connect()
            yield context
        finally:
            if context:
                context.close()

    @contextlib.contextmanager
    def cur(self, **kwargs):
        with self.ctx() as ctx:
            yield ctx.cursor(**kwargs)


@dataclass(frozen=True)
class PrivilegeCheckUser:
    username: str
    host: Optional[str] = None

    @staticmethod
    def parse(s: str):
        parts = s.split("@", maxsplit=1)
        if not parts:
            raise WrongMigrationConfigurationException(f"Error while parsing user {s!r}")

        host = None
        username = parts[0]
        if len(parts) == 2:
            host = parts[1]

        return PrivilegeCheckUser(username=username, host=host)

    @property
    def sql_format(self):
        return "%s@%s" if self.host else "%s"

    @property
    def sql_params(self):
        params = [self.username]
        if self.host:
            params.append(self.host)
        return params


class MySQLDumpProcessor:
    def __init__(self):
        self.gtid = None
        self.gtid_block = ""

    @staticmethod
    def _remove_log_bin_data(line: str) -> str:
        """Remove setting of SQL_LOG_BIN, target might have replicas running, which need to get this data replicated"""
        if line and LOG_BIN_RE.match(line):
            return ""

        return line

    @staticmethod
    def _remove_definers(line: str) -> str:
        """Remove security definers from routines and dump meta, so that the default definer is used"""
        if IMPORT_DEFINER_RE.match(line):
            return ""

        if EXTRA_DEFINER_RE.match(line):
            return EXTRA_DEFINER_RE.sub("\\1\\3", line)

        return ROUTINE_DEFINER_RE.sub("CREATE \\2", line)

    def process_line(self, line: str) -> str:
        if line and not self.gtid:
            if self.gtid_block:
                # Continuation of previous line
                end_match = GTID_END_RE.match(line)
                if end_match:
                    self.gtid_block = self.gtid_block + end_match.group(1)
                    self.gtid = self.gtid_block
                else:
                    self.gtid_block = self.gtid_block + line
                return ""
            else:
                # Search for the start
                start_match = GTID_START_RE.match(line)
                if start_match:
                    if GTID_END_RE.match(line):
                        # One line match
                        self.gtid = start_match.group(1)
                    else:
                        # Multi-line GTID comment
                        self.gtid_block = start_match.group(1)
                    return ""

        line = MySQLDumpProcessor._remove_log_bin_data(line)
        line = MySQLDumpProcessor._remove_definers(line)

        return line

    def get_gtid(self):
        return self.gtid


def select_global_var(cur, var_name: str):
    cur.execute(f"SELECT @@GLOBAL.{var_name} AS VAR")
    return cur.fetchone()["VAR"]
