FROM python:3.10.7 as base

ENV DOCKERIZE_VERSION v0.6.1
ENV LIBRDKAFKA_VERSION=1.6.2
ENV CONFLUENT_KAFKA_VERSION=1.6.1

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y \
    && if [ $(arch) = "aarch64" ]; then \
    DOCKERIZE_ARCH='aarch64';\
    elif [ $(arch) = "x86_64" ]; then \
    DOCKERIZE_ARCH='amd64'; \
    else \
    echo >&2 "Unsupported architecture $(arch)" ; exit 1; \
    fi \
    && apt-get install -y -qq \
    #    gcc \
    make \
    jq \
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
    && curl -L https://github.com/treff7es/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-${DOCKERIZE_ARCH}-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv \
    && python -m pip install --upgrade pip wheel setuptools==57.5.0 \
    && curl -Lk -o /root/librdkafka-${LIBRDKAFKA_VERSION}.tar.gz https://github.com/edenhill/librdkafka/archive/v${LIBRDKAFKA_VERSION}.tar.gz \
    &&  tar -xzf /root/librdkafka-${LIBRDKAFKA_VERSION}.tar.gz -C /root \
    &&  cd /root/librdkafka-${LIBRDKAFKA_VERSION} \
    &&  ./configure --prefix /usr && make && make install && make clean && ./configure --clean \
    && apt-get remove -y make

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

COPY ./base-requirements.txt requirements.txt

RUN pip install -r requirements.txt && \
    pip uninstall -y acryl-datahub
