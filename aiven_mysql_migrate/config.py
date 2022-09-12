# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
import os

MYSQL_CONNECTION_TIMEOUT = 5
MYSQL_WRITE_TIMEOUT = 5
MYSQL_READ_TIMEOUT = 5

IGNORE_SYSTEM_DATABASES = {"mysql", "sys", "information_schema", "performance_schema"}
MYSQL_MAX_DATABASES = 10_000

SOURCE_SERVICE_URI = os.getenv("SOURCE_SERVICE_URI")
TARGET_SERVICE_URI = os.getenv("TARGET_SERVICE_URI")
TARGET_MASTER_SERVICE_URI = os.getenv("TARGET_MASTER_SERVICE_URI")

SOURCE_SSL_CA = os.getenv("SOURCE_SSL_CA")
SOURCE_SSL_CERT = os.getenv("SOURCE_SSL_CERT")
SOURCE_SSL_KEY = os.getenv("SOURCE_SSL_KEY")
