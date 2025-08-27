# Defining custom repo urls for use in enterprise environments
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2
ARG APACHE_DOWNLOAD_URL=null

# Base stage with common dependencies
FROM alpine:3.22 AS base

# Re-declaring args from above
ARG ALPINE_REPO_URL
ARG GITHUB_REPO_URL
ARG MAVEN_CENTRAL_REPO_URL
ARG APACHE_DOWNLOAD_URL

# Kafka specific args
ARG KAFKA_VERSION=4.0.0
ARG SCALA_VERSION=2.13
ARG CONFLUENT_VERSION=8.0.0

# Environment variables
ENV KAFKA_VERSION=${KAFKA_VERSION}
ENV SCALA_VERSION=${SCALA_VERSION}
ENV KAFKA_WORKDIR=/opt/kafka

LABEL name="kafka" version=${KAFKA_VERSION}

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then \
    sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

# Upgrade Alpine and install base packages
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add 'c-ares>1.34.5' --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add \
        bash \
        coreutils \
        curl \
        ca-certificates \
        jq \
        python3 \
        py3-pip \
        wget \
        zip \
        gcompat \
        sqlite \
        libc6-compat \
        snappy=~1.2 --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add openjdk17-jre-headless --repository=${ALPINE_REPO_URL}/edge/community

# Create directories
RUN mkdir -p /opt/kafka /usr/share/java/cp-base-new \
    && cp /usr/lib/jvm/java-17-openjdk/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks

# Download and install Kafka
RUN if [ "${APACHE_DOWNLOAD_URL}" != "null" ] ; then \
        mirror="${APACHE_DOWNLOAD_URL}/" ; \
    else \
        mirror=$(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | jq -r '.preferred'); \
    fi && \
    curl -sSL "${mirror}kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    | tar -xzf - -C /opt && \
    # Check if extraction created the expected directory
    if [ -d "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" ]; then \
        # Move contents properly, not the directory itself
        cp -r /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/* ${KAFKA_WORKDIR}/ && \
        rm -rf /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}; \
    fi

# Download Confluent dependencies
ARG CONFLUENT_REPO_URL=https://packages.confluent.io/maven
RUN wget -P /usr/share/java/cp-base-new \
        ${CONFLUENT_REPO_URL}/io/confluent/common-utils/${CONFLUENT_VERSION}/common-utils-${CONFLUENT_VERSION}.jar \
    && wget -P /usr/share/java/cp-base-new \
        ${CONFLUENT_REPO_URL}/io/confluent/common-config/${CONFLUENT_VERSION}/common-config-${CONFLUENT_VERSION}.jar

# Fix security vulnerabilities (after Kafka is installed so libs directory exists)
ARG COMMONS_BEAN_UTILS_VERSION="1.11.0"
ARG COMMONS_LANG3_VERSION="3.18.0"
RUN wget -P /usr/share/java/cp-base-new \
        ${MAVEN_CENTRAL_REPO_URL}/commons-beanutils/commons-beanutils/${COMMONS_BEAN_UTILS_VERSION}/commons-beanutils-${COMMONS_BEAN_UTILS_VERSION}.jar \
    && rm -f ${KAFKA_WORKDIR}/libs/commons-beanutils-*.jar \
    && cp /usr/share/java/cp-base-new/commons-beanutils-${COMMONS_BEAN_UTILS_VERSION}.jar ${KAFKA_WORKDIR}/libs/ \
    && wget -P /usr/share/java/cp-base-new \
        ${MAVEN_CENTRAL_REPO_URL}/org/apache/commons/commons-lang3/${COMMONS_LANG3_VERSION}/commons-lang3-${COMMONS_LANG3_VERSION}.jar \
    && rm -f ${KAFKA_WORKDIR}/libs/commons-lang3-*.jar \
    && cp /usr/share/java/cp-base-new/commons-lang3-${COMMONS_LANG3_VERSION}.jar ${KAFKA_WORKDIR}/libs/

# Download AWS MSK IAM Auth
ADD ${GITHUB_REPO_URL}/aws/aws-msk-iam-auth/releases/download/v2.3.2/aws-msk-iam-auth-2.3.2-all.jar /usr/share/java/cp-base-new/
ADD ${GITHUB_REPO_URL}/aws/aws-msk-iam-auth/releases/download/v2.3.2/aws-msk-iam-auth-2.3.2-all.jar ${KAFKA_WORKDIR}/libs/

# Set LD_LIBRARY_PATH for compatibility
ENV LD_LIBRARY_PATH="/lib:/lib64"

# Copy setup scripts
COPY docker/kafka-setup/kafka-setup.sh ${KAFKA_WORKDIR}/kafka-setup.sh
COPY docker/kafka-setup/kafka-config.sh ${KAFKA_WORKDIR}/kafka-config.sh
COPY docker/kafka-setup/kafka-topic-workers.sh ${KAFKA_WORKDIR}/kafka-topic-workers.sh
COPY docker/kafka-setup/kafka-ready.sh ${KAFKA_WORKDIR}/kafka-ready.sh
COPY docker/kafka-setup/env_to_properties.py ${KAFKA_WORKDIR}/env_to_properties.py

# Make scripts executable
RUN chmod +x ${KAFKA_WORKDIR}/kafka-setup.sh \
    ${KAFKA_WORKDIR}/kafka-topic-workers.sh \
    ${KAFKA_WORKDIR}/kafka-ready.sh

# Create kafka user and group
RUN addgroup -S kafka && adduser -S kafka -G kafka && chmod g-s /home/kafka

# Set ownership
RUN chown -R kafka:kafka ${KAFKA_WORKDIR} \
    && chown -R kafka:kafka /usr/share/java/cp-base-new

# Switch to kafka user
USER kafka

# Set environment variables for DataHub
ENV METADATA_AUDIT_EVENT_NAME="MetadataAuditEvent_v4"
ENV METADATA_CHANGE_EVENT_NAME="MetadataChangeEvent_v4"
ENV FAILED_METADATA_CHANGE_EVENT_NAME="FailedMetadataChangeEvent_v4"
ENV DATAHUB_USAGE_EVENT_NAME="DataHubUsageEvent_v1"
ENV METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME="MetadataChangeLog_Versioned_v1"
ENV METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME="MetadataChangeLog_Timeseries_v1"
ENV METADATA_CHANGE_PROPOSAL_TOPIC_NAME="MetadataChangeProposal_v1"
ENV FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME="FailedMetadataChangeProposal_v1"
ENV PLATFORM_EVENT_TOPIC_NAME="PlatformEvent_v1"
ENV DATAHUB_UPGRADE_HISTORY_TOPIC_NAME="DataHubUpgradeHistory_v1"
ENV USE_CONFLUENT_SCHEMA_REGISTRY="TRUE"

# Set PATH
ENV PATH=/sbin:${KAFKA_WORKDIR}/bin:$PATH

# Set working directory
WORKDIR ${KAFKA_WORKDIR}

CMD ["./kafka-setup.sh"]