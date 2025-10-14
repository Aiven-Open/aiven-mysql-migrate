FROM python:3.12-bookworm

RUN apt-get update && apt-get install -y curl gnupg \
    && curl -fsSL https://repo.mysql.com/RPM-GPG-KEY-mysql-2023 | gpg --dearmor -o /usr/share/keyrings/mysql-keyring.gpg \
    && echo "deb [trusted=yes signed-by=/usr/share/keyrings/mysql-keyring.gpg] http://repo.mysql.com/apt/debian/ bookworm mysql-8.0" | tee /etc/apt/sources.list.d/mysql.list \
    && apt-get update \
    && apt-get install -y mysql-client

# Install mydumper/myloader
RUN apt-get install -y wget
RUN wget https://github.com/mydumper/mydumper/releases/download/v0.20.1-2/mydumper_0.20.1-2.bookworm_amd64.deb
RUN apt-get install -y ./mydumper_0.20.1-2.bookworm_amd64.deb
RUN apt-get install -y zstd  # Required for compression

COPY . /app
WORKDIR /app

RUN pip install --upgrade pip
RUN pip install -e ".[dev]"
