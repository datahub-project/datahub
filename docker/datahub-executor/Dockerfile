# Defining environment
ARG APP_ENV=prod
ARG BASE_IMAGE=acryldata/datahub-ingestion
ARG DOCKER_VERSION=head-slim
ARG PIP_MIRROR_URL=null
ARG VIRTUAL_ENV=/datahub-ingestion/.venv

# Base Image Common To All
FROM $BASE_IMAGE:$DOCKER_VERSION AS base
USER 0

FROM base AS build

COPY ./datahub-executor /datahub-executor
COPY ./metadata-ingestion /metadata-ingestion
COPY ./metadata-ingestion-modules/acryl-cloud /acryl-cloud
COPY ./docker/datahub-executor/start.sh /start_datahub_executor.sh
COPY ./docker/datahub-executor/health_status.py /health_status
COPY ./docker/datahub-executor/ingestion-pipeline-config.yaml /etc/datahub-executor/ingestion-pipeline-config.yaml
RUN chmod a+x /start_datahub_executor.sh
RUN chmod a+x /health_status

WORKDIR /datahub-executor

ARG DOCKER_VERSION
RUN chown -R datahub /metadata-ingestion /datahub-executor /acryl-cloud

ARG PIP_MIRROR_URL
RUN if [ "${PIP_MIRROR_URL}" != "null" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi

# Install JRE in the prod image
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
RUN ln -svT "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/default-jvm
ENV JAVA_HOME=/usr/lib/jvm/default-jvm

# Separate curl + git installation
RUN apt-get update \
    && apt-get install -y curl git jq \
    && apt-get clean \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

# Install Microsoft SQL Server ODBC driver
RUN <<EOF
set -e

. /etc/os-release

case $ID in
  ubuntu)
    UBUNTU_VERSION=$(lsb_release -rs)
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
    curl -sSL https://packages.microsoft.com/config/ubuntu/$UBUNTU_VERSION/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
    ;;

  debian)
    DEBIAN_VERSION=$(cat /etc/debian_version | cut -d . -f 1)
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
    curl -sSL https://packages.microsoft.com/config/debian/$DEBIAN_VERSION/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
    gpg -o /usr/share/keyrings/microsoft-prod.gpg --dearmor /etc/apt/trusted.gpg.d/microsoft.asc
    ;;

  *) echo "This is an unknown distribution."
    ;;
esac

apt-get update
ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18
rm -rf /var/lib/apt/lists/*

odbcinst -j || { echo 'odbcinst -j failed'; exit 1; }
odbcinst -q -d -n "ODBC Driver 18 for SQL Server" || { echo 'ODBC Driver 18 for SQL Server not found'; exit 1; }

EOF

# Install Oracle Instant Client based on architecture
RUN if [ $(arch) = "x86_64" ]; then \
    mkdir -p /opt/oracle && \
    cd /opt/oracle && \
    wget --no-verbose -c https://download.oracle.com/otn_software/linux/instantclient/216000/instantclient-basic-linux.x64-21.6.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.x64-21.6.0.0.0dbru.zip && \
    rm instantclient-basic-linux.x64-21.6.0.0.0dbru.zip && \
    sh -c "echo /opt/oracle/instantclient_21_6 > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig; \
    else \
    mkdir -p /opt/oracle && \
    cd /opt/oracle && \
    wget --no-verbose -c https://download.oracle.com/otn_software/linux/instantclient/191000/instantclient-basic-linux.arm64-19.10.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.arm64-19.10.0.0.0dbru.zip && \
    rm instantclient-basic-linux.arm64-19.10.0.0.0dbru.zip && \
    sh -c "echo /opt/oracle/instantclient_19_10 > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig; \
    fi;

# Embed build info into the image
RUN jq -n '{"docker_version": env.DOCKER_VERSION, "release_version": env.RELEASE_VERSION}' > /build_info.json

USER datahub
RUN uv pip install --no-cache -e .

## Production Image Built From Build Image
FROM build AS prod-install

USER datahub

FROM prod-install AS dev-install

# Dummy stage for development. Assumes most of the code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

ENV PYTHONPATH="/metadata-ingestion/src:/datahub-executor/src/datahub_executor:${PYTHONPATH}"
ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-executor/src/datahub_executor --reload-dir /metadata-ingestion/src"

FROM ${APP_ENV}-install AS final

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9004/health || exit 1
ENTRYPOINT []
CMD exec dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_executor.sh
