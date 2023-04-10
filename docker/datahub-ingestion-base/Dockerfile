FROM golang:1-alpine3.17 AS binary

ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /go/src/github.com/jwilder

RUN apk --no-cache --update add openssl git tar curl

WORKDIR /go/src/github.com/jwilder/dockerize

RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION

FROM python:3.10 as base

ENV LIBRDKAFKA_VERSION=1.6.2
ENV CONFLUENT_KAFKA_VERSION=1.6.1

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y \
    && apt-get install -y -qq \
    #    gcc \
    make \
    python3-ldap \
    libldap2-dev \
    libsasl2-dev \
    libsasl2-modules \
    libaio1 \
    libsasl2-modules-gssapi-mit \
    krb5-user \
    wget \
    zip \
    unzip \
    ldap-utils \
    openjdk-11-jre-headless \
    && python -m pip install --upgrade pip wheel setuptools==57.5.0 \
    && curl -Lk -o /root/librdkafka-${LIBRDKAFKA_VERSION}.tar.gz https://github.com/edenhill/librdkafka/archive/v${LIBRDKAFKA_VERSION}.tar.gz \
    &&  tar -xzf /root/librdkafka-${LIBRDKAFKA_VERSION}.tar.gz -C /root \
    &&  cd /root/librdkafka-${LIBRDKAFKA_VERSION} \
    &&  ./configure --prefix /usr && make && make install && make clean && ./configure --clean \
    && apt-get remove -y make
COPY --from=binary /go/bin/dockerize /usr/local/bin

RUN if [ $(arch) = "x86_64" ]; then \
    mkdir /opt/oracle && \
    cd /opt/oracle && \
    wget --no-verbose -c https://download.oracle.com/otn_software/linux/instantclient/216000/instantclient-basic-linux.x64-21.6.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.x64-21.6.0.0.0dbru.zip && \
    rm instantclient-basic-linux.x64-21.6.0.0.0dbru.zip && \
    sh -c "echo /opt/oracle/instantclient_21_6 > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig; \
    else \
    mkdir /opt/oracle && \
    cd /opt/oracle && \
    wget --no-verbose -c https://download.oracle.com/otn_software/linux/instantclient/191000/instantclient-basic-linux.arm64-19.10.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.arm64-19.10.0.0.0dbru.zip && \
    rm instantclient-basic-linux.arm64-19.10.0.0.0dbru.zip && \
    sh -c "echo /opt/oracle/instantclient_19_10 > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig; \
    fi;

COPY ./docker/datahub-ingestion-base/base-requirements.txt requirements.txt

RUN pip install -r requirements.txt && \
    pip uninstall -y acryl-datahub
