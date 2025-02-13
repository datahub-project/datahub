ARG KAFKA_DOCKER_VERSION=7.8.1

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2
ARG APACHE_DOWNLOAD_URL=null

# Using AS a base image because to get the needed jars for confluent utils
FROM confluentinc/cp-base-new:$KAFKA_DOCKER_VERSION AS confluent_base

ARG MAVEN_CENTRAL_REPO_URL
ARG SNAKEYAML_VERSION="2.0"

RUN rm /usr/share/java/cp-base-new/snakeyaml-*.jar \
  && wget -P /usr/share/java/cp-base-new $MAVEN_CENTRAL_REPO_URL/org/yaml/snakeyaml/$SNAKEYAML_VERSION/snakeyaml-$SNAKEYAML_VERSION.jar

# Based on https://github.com/blacktop's alpine kafka build
FROM python:3-alpine

ARG ALPINE_REPO_URL
ARG APACHE_DOWNLOAD_URL
ARG GITHUB_REPO_URL

ENV KAFKA_VERSION=3.7.2
ENV SCALA_VERSION=2.13

LABEL name="kafka" version=${KAFKA_VERSION}

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

RUN apk add --no-cache bash coreutils
RUN apk --no-cache add openjdk17-jre-headless --repository=${ALPINE_REPO_URL}/edge/community

RUN apk add --no-cache -t .build-deps git curl ca-certificates jq gcc musl-dev libffi-dev zip
RUN mkdir -p /opt \
  && if [ "${APACHE_DOWNLOAD_URL}" != "null" ] ; then mirror="${APACHE_DOWNLOAD_URL}/" ; else mirror=$(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | jq -r '.preferred'); fi \
  && curl -sSL "${mirror}kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
  | tar -xzf - -C /opt \
  && mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
  && adduser -DH -s /sbin/nologin kafka \
  && chown -R kafka: /opt/kafka \
  && rm -rf /tmp/* \
  && apk del --purge .build-deps

ENV PATH=/sbin:/opt/kafka/bin/:$PATH

WORKDIR /opt/kafka

RUN ls -la
COPY --from=confluent_base /usr/share/java/cp-base-new/ /usr/share/java/cp-base-new/
COPY --from=confluent_base /etc/cp-base-new/log4j.properties /etc/cp-base-new/log4j.properties

ADD --chown=kafka:kafka ${GITHUB_REPO_URL}/aws/aws-msk-iam-auth/releases/download/v2.0.3/aws-msk-iam-auth-2.0.3-all.jar /usr/share/java/cp-base-new
ADD --chown=kafka:kafka ${GITHUB_REPO_URL}/aws/aws-msk-iam-auth/releases/download/v2.0.3/aws-msk-iam-auth-2.0.3-all.jar /opt/kafka/libs

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

COPY docker/kafka-setup/kafka-setup.sh ./kafka-setup.sh
COPY docker/kafka-setup/kafka-config.sh ./kafka-config.sh
COPY docker/kafka-setup/kafka-topic-workers.sh ./kafka-topic-workers.sh
COPY docker/kafka-setup/kafka-ready.sh ./kafka-ready.sh
COPY docker/kafka-setup/env_to_properties.py ./env_to_properties.py

RUN chmod +x ./kafka-setup.sh ./kafka-topic-workers.sh ./kafka-ready.sh

CMD ./kafka-setup.sh
