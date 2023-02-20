# Using as a base image because to get the needed jars for confluent utils
FROM confluentinc/cp-base-new@sha256:ac4e0f9bcaecdab728740529f37452231fa40760fcf561759fc3b219f46d2cc9 as confluent_base

ARG MAVEN_REPO="https://repo1.maven.org/maven2"
ARG SNAKEYAML_VERSION="1.33"

RUN rm /usr/share/java/cp-base-new/snakeyaml-*.jar \
    && wget -P /usr/share/java/cp-base-new $MAVEN_REPO/org/yaml/snakeyaml/$SNAKEYAML_VERSION/snakeyaml-$SNAKEYAML_VERSION.jar

# Based on https://github.com/blacktop's alpine kafka build
FROM python:3-alpine

ENV KAFKA_VERSION 3.4.0
ENV SCALA_VERSION 2.13

# Set the classpath for JARs required by `cub`
ENV CUB_CLASSPATH='"/usr/share/java/cp-base-new/*"'

# Confluent Docker Utils Version (Namely the tag or branch to grab from git to install)
ARG PYTHON_CONFLUENT_DOCKER_UTILS_VERSION="v0.0.58"

# This can be overriden for an offline/air-gapped builds
ARG PYTHON_CONFLUENT_DOCKER_UTILS_INSTALL_SPEC="git+https://github.com/confluentinc/confluent-docker-utils@${PYTHON_CONFLUENT_DOCKER_UTILS_VERSION}"

LABEL name="kafka" version=${KAFKA_VERSION}

RUN apk add --no-cache bash coreutils
RUN apk --no-cache add openjdk11-jre --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community

RUN apk add --no-cache -t .build-deps git curl ca-certificates jq gcc musl-dev libffi-dev zip
RUN mkdir -p /opt \
  && mirror=$(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | jq -r '.preferred') \
  && curl -sSL "${mirror}kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
  | tar -xzf - -C /opt \
  && mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
  && adduser -DH -s /sbin/nologin kafka \
  && chown -R kafka: /opt/kafka \
  && echo "===> Installing python packages ..."  \
  && pip install --no-cache-dir jinja2 requests \
  && pip install --prefer-binary --prefix=/usr/local --upgrade "${PYTHON_CONFLUENT_DOCKER_UTILS_INSTALL_SPEC}" \
  && rm -rf /tmp/* \
  && apk del --purge .build-deps

ENV PATH /sbin:/opt/kafka/bin/:$PATH

WORKDIR /opt/kafka

RUN ls -la
COPY --from=confluent_base /usr/share/java/cp-base-new/ /usr/share/java/cp-base-new/
COPY --from=confluent_base /etc/cp-base-new/log4j.properties /etc/cp-base-new/log4j.properties

ADD --chown=kafka:kafka https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.6/aws-msk-iam-auth-1.1.6-all.jar /usr/share/java/cp-base-new
ADD --chown=kafka:kafka https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.6/aws-msk-iam-auth-1.1.6-all.jar /opt/kafka/libs

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

COPY docker/kafka-setup/kafka-setup.sh ./kafka-setup.sh
COPY docker/kafka-setup/kafka-config.sh ./kafka-config.sh
COPY docker/kafka-setup/kafka-topic-workers.sh ./kafka-topic-workers.sh

RUN chmod +x ./kafka-setup.sh && chmod +x ./kafka-topic-workers.sh

CMD ./kafka-setup.sh
