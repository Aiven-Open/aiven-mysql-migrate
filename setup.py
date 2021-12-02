# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from importlib.machinery import SourceFileLoader
from setuptools import find_packages, setup


def get_version():
    return SourceFileLoader("version", "aiven_mysql_migrate/version.py").load_module().__version__


def get_long_description():
    with open("README.md") as desc_file:
        return desc_file.read()


setup(
    author="Aiven",
    author_email="support@aiven.io",
    entry_points={
        "console_scripts": [
            "mysql_migrate = aiven_mysql_migrate.main:main",
        ],
    },
    install_requires=[
        "pymysql~=0.10.0"
    ],
    license="Apache 2.0",
    name="aiven-mysql-migrate",
    packages=find_packages(exclude=["test"]),
    platforms=["POSIX"],
    description="Aiven MySQL database migration tool",
    long_description=get_long_description(),
    url="https://aiven.io/",
    version=get_version(),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
    ],
)
