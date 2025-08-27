# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_mysql_migrate import config
from aiven_mysql_migrate.exceptions import NothingToMigrateException
from aiven_mysql_migrate.migration import MySQLMigrateMethod, MySQLMigration
from pathlib import Path
from typing import Optional, Sequence

import logging

LOGGER = logging.getLogger(__name__)


def setup_logging(*, debug=False):
    log_format = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s"
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=log_level, format=log_format)


def main(args: Sequence[str] | None = None, *, app: str = "mysql_migrate") -> Optional[str]:
    """Migrate MySQL database from source to target, take configuration from CONFIG"""
    import argparse
    parser = argparse.ArgumentParser(description="MySQL migration tool.", prog=app)
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument(
        "-f", "--filter-dbs", help="Comma separated list of databases to filter out during migration", required=False
    )
    parser.add_argument("--validate-only", action="store_true", help="Run migration pre-checks only")
    parser.add_argument(
        "--seconds-behind-master",
        type=int,
        default=-1,
        help="Max replication lag in seconds to wait for, by default no wait"
    )
    parser.add_argument(
        "--stop-replication", action="store_true", help="Stop replication, by default replication is left running"
    )
    parser.add_argument(
        "--privilege-check-user",
        type=str,
        required=False,
        help="User to be used when replicating for privileges check "
        "(e.g. 'checker@%%', must have REPLICATION_APPLIER grant)"
    )
    parser.add_argument(
        "--force-method",
        type=str,
        required=False,
        default=None,
        help="Force the migration method to be used as either replication or dump."
    )
    parser.add_argument(
        "--dbs-max-total-size",
        type=int,
        default=-1,
        help="Max total size of databases to be migrated, ignored by default",
    )
    parser.add_argument(
        "--output-meta-file",
        type=Path,
        required=False,
        default=None,
        help="Output file which includes metadata such as dump GTIDs (for replication method only) in JSON format.",
    )
    parser.add_argument(
        "--allow-source-without-dbs",
        action="store_true",
        help="Allow migrating from a source that has no migratable databases"
    )
    parsed_args = parser.parse_args(args)
    setup_logging(debug=parsed_args.debug)

    assert config.SOURCE_SERVICE_URI, "SOURCE_SERVICE_URI is not specified"
    assert config.TARGET_SERVICE_URI, "TARGET_SERVICE_URI is not specified"

    migration = MySQLMigration(
        source_uri=config.SOURCE_SERVICE_URI,
        target_uri=config.TARGET_SERVICE_URI,
        target_master_uri=config.TARGET_MASTER_SERVICE_URI,
        filter_dbs=parsed_args.filter_dbs,
        privilege_check_user=parsed_args.privilege_check_user,
        output_meta_file=parsed_args.output_meta_file,
    )
    migration.setup_signal_handlers()

    LOGGER.info("MySQL migration from %s to %s", migration.source.hostname, migration.target.hostname)

    LOGGER.info("Starting pre-checks")
    dbs_max_total_size = None if parsed_args.dbs_max_total_size == -1 else parsed_args.dbs_max_total_size
    try:
        migration_method = migration.run_checks(force_method=parsed_args.force_method, dbs_max_total_size=dbs_max_total_size)
    except NothingToMigrateException:
        if not parsed_args.allow_source_without_dbs:
            raise

        LOGGER.warning("No databases to migrate.")
        return None

    expected_method = MySQLMigrateMethod.replication if parsed_args.force_method is None else parsed_args.force_method
    if migration_method == expected_method:
        LOGGER.info("All pre-checks passed successfully.")
    else:
        LOGGER.info("Not all pre-checks passed successfully. %s method is not available.", expected_method.capitalize())
        # We were unable to use the desired method so all we can do here is exit.
        if parsed_args.force_method is not None:
            return f"{expected_method.capitalize()} method is not available."

    if parsed_args.validate_only:
        return None

    LOGGER.info("Starting migration using method: %s", migration_method)
    migration.start(
        migration_method=migration_method,
        seconds_behind_master=parsed_args.seconds_behind_master,
        stop_replication=parsed_args.stop_replication,
    )

    LOGGER.info("Migration finished.")
    if migration_method == MySQLMigrateMethod.replication and not parsed_args.stop_replication:
        LOGGER.info("IMPORTANT: Replication is still running, make sure to stop it after switching to the target DB")

    return None


if __name__ == "__main__":
    import sys
    sys.exit(main())
