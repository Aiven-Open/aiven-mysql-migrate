FROM python:3.7

RUN apt-get update -y
RUN apt-get install -y expect
RUN curl -L 'https://dev.mysql.com/get/mysql-apt-config_0.8.16-1_all.deb' -o mysql-apt-config_0.8.16-1_all.deb
ADD ./install-mysql-config.exp /
RUN /install-mysql-config.exp
RUN apt-get update -y && apt-get install -y mysql-client

COPY . /app
WORKDIR /app

RUN pip install --upgrade pip
RUN pip install -r /app/requirement-dev.txt
RUN python setup.py install
