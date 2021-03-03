# aiven-mysql-migrate
The aim of this tool is to help migrate MySQL servers from one place to another.

`aiven-mysql-migrate` supports two methods of migration:
* `mysqldump + replication`

    This method is preferred for production databases because it allows migrating data with a minimal downtime.

    The way it works - `aiven-mysql-migrate` checks if all preconditions are met for this method and starts coping
  initial data dump from the source database to the target using `mysqldump/mysql` tools. This will not lock the origin
  database. Once the initial dump is copied, the target database will set up the replication from the source and set
  the GTID (Global Transaction Identifier) from the dump. This will let the target database start getting changes from
  the source database which were made while initial dump migration. When the source and target database are in sync,
  the user can stop the source database and switch to using the target database. The replication can be stopped at this
  point.

    Pre-conditions:
    * The source database should be in >= 5.7 and <= 8.0
    * The target database should have at least version 8.0
    * All databases have the same engine - InnoDB
    * `gtid_mode` is `ON` on both the source and the target
    * Master user for replication management is specified (via `TARGET_MASTER_SERVICE_URI`)
    * User on the source database has enough permissions to create a replication user and read data
    * `server_id` on the source and the target do not overlap

    If one of the pre-conditions is not met, the migration is falling back to the second method `mysqldump`


* `mysqldump` only
    Works the same as previous method except replication is not setup. The drawback is that the source database should
  not be used while migrating, otherwise data will be inconsistent.

## Requirements
The tool requires `mysql-client` package 8.X, which can be installed from https://dev.mysql.com/doc/refman/8.0/en/linux-installation.html

## Limitations
* No support for multi-master cluster
* Target DB versions: >= 8, source DB: >= 5.7.
* InnoDB engine: if MyISAM engine is used (even partially) replication method will not work
* System databases/tables (`information_schema.*, mysql.*, performance_schema.*, sys.*`) are excluded from migration
  (DB users need to be pre-created on a target database beforehand).
* Functions/triggers/events will be created during the initial import, however they won't be synchronized while the
  import is in progress, so there should be no changes done to those.
* No schema changes should be made while doing migration.
* Security definers are not transferred from the source database, they will be replaced with the import user
  (from `TARGET_SERVICE_URI`).

## Usage
```
mysql_migrate --help
usage: mysql_migrate [-h] [--log-level {debug,info,warning,error}] [-f FILTER_DBS] [--validate-only] [--json] [--json-file JSON_FILE]
                     [--seconds-behind-master SECONDS_BEHIND_MASTER] [--stop-replication] [--privilege-check-user PRIVILEGE_CHECK_USER]

MySQL migration tool.

optional arguments:
  -h, --help            show this help message and exit
  --log-level {debug,info,warning,error}
                        Change log level
  -f FILTER_DBS, --filter-dbs FILTER_DBS
                        Comma separated list of databases to filter out during migration
  --validate-only       Run migration pre-checks only
  --json                Print the output as json, only used in combination with --validate-style
  --json-file JSON_FILE
                        File-path to write the json output and skip stdout, mutually exclusive to --json and only used in combination with --validate-style
  --seconds-behind-master SECONDS_BEHIND_MASTER
                        Max replication lag in seconds to wait for, by default no wait
  --stop-replication    Stop replication, by default replication is left running
  --privilege-check-user PRIVILEGE_CHECK_USER
                        User to be used when replicating for privileges check (e.g. 'checker@%', must have REPLICATION_APPLIER grant)
```

The following environment variables are used by migration script:
* `SOURCE_SERVICE_URI` - service URI to the source MySQL database with admin credentials
* `TARGET_SERVICE_URI` - service URI to the target MySQL database with admin credentials, which will be used for dump import.
* `TARGET_MASTER_SERVICE_URI` - service URI for managing replication while migrating, omitting this variable will
lead to fall-back to dump solution.

Environment variable are used here instead of usual arguments so that it's not possible to see credentials in the list
of long-running processes. As for the `mysqldump/mysql` subprocesses they won't be visible, because they are hidden by
the tools.

The reason for having a separate master URI is a security concern, i.e. for properly managing replication while migrating
the super user is needed, which we should not use for importing MySQL dumps, because the SQL statements from the dump
will be executed in the context for this user.

## Run pre-checks to validate if migration is possible:
```bash
SOURCE_SERVICE_URI="mysql://<src_admin>:<pwd>@<src_host>:<port>/?ssl-mode=REQUIRED" \
  TARGET_SERVICE_URI="mysql://<tag_admin>:<pwd>@<tag_host>:<port>/?ssl-mode=REQUIRED" \
  TARGET_MASTER_SERVICE_URI="mysql://<tag_superuser>:<pwd>@<tag_host>:<port>/?ssl-mode=REQUIRED" \
  mysql_migrate --validate-only
```

`ssl-mode` parameter can be one of `DISABLE` or `REQUIRED`

## Migrate:
```bash
mysql_migrate --filter-dbs "<temp_db1>,<temp_db2>" --seconds-behind-master 0 --stop-replication
```
