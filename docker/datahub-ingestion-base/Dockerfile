ARG APP_ENV=full
ARG BASE_IMAGE=base

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG DEBIAN_REPO_URL=https://deb.debian.org/debian
ARG PIP_MIRROR_URL=https://pypi.python.org/simple

FROM golang:1-alpine3.18 AS dockerize-binary

# Re-declaring arg from above to make it available in this stage (will inherit default value)
ARG ALPINE_REPO_URL

ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /go/src/github.com/jwilder

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

RUN apk --no-cache --update add openssl git tar curl

WORKDIR /go/src/github.com/jwilder/dockerize

RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION

FROM python:3.10 as base

ARG GITHUB_REPO_URL

ENV DEBIAN_FRONTEND noninteractive

# Optionally set corporate mirror for deb
ARG DEBIAN_REPO_URL
RUN if [ "${DEBIAN_REPO_URL}" != "http://deb.debian.org/debian" ] ; then sed -i "s#http.*://deb.debian.org/debian#${DEBIAN_REPO_URL}#g" /etc/apt/sources.list.d/debian.sources ; fi

# Optionally set corporate mirror for pip
ARG PIP_MIRROR_URL
RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi
ENV UV_INDEX_URL=${PIP_MIRROR_URL}

RUN apt-get update && apt-get install -y -qq \
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
    unixodbc \
    libodbc2 \
    && python -m pip install --no-cache --upgrade pip uv>=0.1.10 wheel setuptools \
    && rm -rf /var/lib/apt/lists/* /var/cache/apk/*

# compiled against newer golang for security fixes
COPY --from=dockerize-binary /go/bin/dockerize /usr/local/bin

COPY ./docker/datahub-ingestion-base/base-requirements.txt requirements.txt
COPY ./docker/datahub-ingestion-base/entrypoint.sh /entrypoint.sh

RUN addgroup --gid 1000 datahub && \
    adduser --disabled-password --uid 1000 --gid 1000 --home /datahub-ingestion datahub && \
    chmod +x /entrypoint.sh

USER datahub
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV VIRTUAL_ENV=/datahub-ingestion/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
RUN python3 -m venv $VIRTUAL_ENV && \
    uv pip install --no-cache -r requirements.txt

ENTRYPOINT [ "/entrypoint.sh" ]

FROM ${BASE_IMAGE} as full-install

USER 0
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

USER datahub

FROM ${BASE_IMAGE} as slim-install
# Do nothing else on top of base

FROM ${APP_ENV}-install

ENV PATH="/datahub-ingestion/.local/bin:$PATH"
