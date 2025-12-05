FROM python:3.12-bookworm

RUN gpg --keyserver keyserver.ubuntu.com --recv-keys A8D3785C \
    && gpg --export A8D3785C > /etc/apt/keyrings/mysql.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/mysql.gpg] http://repo.mysql.com/apt/debian/ bookworm mysql-8.0" | tee /etc/apt/sources.list.d/mysql.list \
    && apt-get update -y \
    && apt-get install -y mysql-client

# Install mydumper/myloader
RUN apt-get install -y wget zstd
RUN wget https://github.com/mydumper/mydumper/releases/download/v0.21.1-1/mydumper_0.21.1-1.bookworm_amd64.deb
RUN apt-get install -y ./mydumper_0.21.1-1.bookworm_amd64.deb

COPY . /app
WORKDIR /app

RUN pip install --upgrade pip
RUN pip install -e ".[dev]"
