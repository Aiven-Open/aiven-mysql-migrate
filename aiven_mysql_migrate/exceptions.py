# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
class MigrationPreCheckException(Exception):
    pass


class EndpointConnectionException(MigrationPreCheckException):
    pass


class WrongMigrationConfigurationException(MigrationPreCheckException):
    pass


class TooManyDatabasesException(MigrationPreCheckException):
    pass


class DatabaseTooLargeException(MigrationPreCheckException):
    pass


class NothingToMigrateException(MigrationPreCheckException):
    pass


class ReplicationNotAvailableException(Exception):
    pass


class UnsupportedMySQLVersionException(ReplicationNotAvailableException):
    pass


class MissingReplicationGrants(ReplicationNotAvailableException):
    pass


class UnsupportedMySQLEngineException(ReplicationNotAvailableException):
    pass


class GTIDModeDisabledException(ReplicationNotAvailableException):
    pass


class ServerIdsOverlappingException(ReplicationNotAvailableException):
    pass


class UnsupportedBinLogFormatException(ReplicationNotAvailableException):
    pass


class MySQLDumpException(Exception):
    pass


class MySQLImportException(Exception):
    pass


class ReplicaSetupException(Exception):
    pass
