FROM python:3.9.9 as base

RUN apt-get update && apt-get install -y \
        jq \
        librdkafka-dev \
        python3-ldap \
        libldap2-dev \
        libsasl2-dev \
        libsasl2-modules \
        ldap-utils \
    && python -m pip install --upgrade pip wheel setuptools==57.5.0

COPY ./base-requirements.txt requirements.txt

RUN pip install -r requirements.txt