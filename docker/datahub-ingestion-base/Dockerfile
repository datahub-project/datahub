ARG APP_ENV=full
ARG BASE_IMAGE=base

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

RUN apt-get update && apt-get install -y -qq \
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
    && python -m pip install --no-cache --upgrade pip wheel setuptools \
    && wget -q https://github.com/edenhill/librdkafka/archive/v${LIBRDKAFKA_VERSION}.tar.gz -O - | \
          tar -xz -C /root \
    && cd /root/librdkafka-${LIBRDKAFKA_VERSION} \
    && ./configure --prefix /usr && make && make install && cd .. && rm -rf /root/librdkafka-${LIBRDKAFKA_VERSION} \
    && apt-get remove -y make \
    && rm -rf /var/lib/apt/lists/* /var/cache/apk/*

# compiled against newer golang for security fixes
COPY --from=binary /go/bin/dockerize /usr/local/bin

COPY ./docker/datahub-ingestion-base/base-requirements.txt requirements.txt
COPY ./docker/datahub-ingestion-base/entrypoint.sh /entrypoint.sh

RUN pip install --no-cache -r requirements.txt && \
    pip uninstall -y acryl-datahub && \
    chmod +x /entrypoint.sh && \
    addgroup --gid 1000 datahub && \
    adduser --disabled-password --uid 1000 --gid 1000 --home /datahub-ingestion datahub

ENTRYPOINT [ "/entrypoint.sh" ]

FROM ${BASE_IMAGE} as full-install

RUN apt-get update && apt-get install -y -qq \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/* /var/cache/apk/*

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

FROM ${BASE_IMAGE} as slim-install
# Do nothing else on top of base

FROM ${APP_ENV}-install

USER datahub
ENV PATH="/datahub-ingestion/.local/bin:$PATH"