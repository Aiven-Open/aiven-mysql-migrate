## Changelog

## [unreleased] - 2021-03-xx
- Introduced two new flags `--json` and `--json-file` which can be used to verify the replication method used in json format.
    ```
    # json output 
    mysql_migrate --validate-only --json --log-level error
    {"method": "dump", "reason": "Replication method is not available due to missing TARGET_MASTER_SERVICE_URI, falling back to dump"}

    # or

    mysql_migrate --validate-only --json-file=/tmp/outout.json --log-level=error
    ❯ cat /tmp/outout.json 
    {"method": "dump", "status": "Replication method is not available due to missing TARGET_MASTER_SERVICE_URI, falling back to dump"}
    ```