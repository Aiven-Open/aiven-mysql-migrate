FROM python:3.12-bookworm

RUN gpg --keyserver keyserver.ubuntu.com --recv-keys A8D3785C \
    && gpg --export A8D3785C > /etc/apt/keyrings/mysql.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/mysql.gpg] http://repo.mysql.com/apt/debian/ bookworm mysql-8.0" | tee /etc/apt/sources.list.d/mysql.list \
    && apt-get update -y \
    && apt-get install -y mysql-client

COPY . /app
WORKDIR /app

RUN pip install --upgrade pip
RUN pip install -e ".[dev]"