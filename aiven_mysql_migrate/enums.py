from enum import Enum


class MySQLMigrateTool(str, Enum):
    mysqldump = "mysqldump"
    mydumper = "mydumper"


class MySQLMigrateMethod(str, Enum):
    dump = "dump"
    replication = "replication"
