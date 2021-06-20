FROM python:3.8 as base
RUN apt-get update && apt-get install -y \
        librdkafka-dev \
        python3-ldap \
        libldap2-dev \
        libsasl2-dev \
        ldap-utils \
    && python3 -m pip install --user --upgrade pip \
    && python3 -m pip install --user virtualenv \
    && source env/bin/activate
COPY ../../metadata-ingestion/setup.py /setup.py
RUN pip install --use-feature=in-tree-build .[all] fastapi[all]
COPY ./ingest-api /ingest-api && cd /ingest-api
CMD ["python"]