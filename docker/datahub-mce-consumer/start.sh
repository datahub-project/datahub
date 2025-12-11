#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


WAIT_FOR_KAFKA=""
if [[ $SKIP_KAFKA_CHECK != true ]]; then
  WAIT_FOR_KAFKA=" -wait tcp://$(echo $KAFKA_BOOTSTRAP_SERVER | sed 's/,/ -wait tcp:\/\//g') "
fi

WAIT_FOR_SCHEMA_REGISTRY=""
if [[ "$KAFKA_SCHEMAREGISTRY_URL" && $SKIP_SCHEMA_REGISTRY_CHECK != true ]]; then
  WAIT_FOR_SCHEMA_REGISTRY="-wait $KAFKA_SCHEMAREGISTRY_URL"
fi

OTEL_AGENT=""
if [[ $ENABLE_OTEL == true ]]; then
  OTEL_AGENT="-javaagent:opentelemetry-javaagent.jar "
fi

PROMETHEUS_AGENT=""
if [[ $ENABLE_PROMETHEUS == true ]]; then
  PROMETHEUS_AGENT="-javaagent:jmx_prometheus_javaagent.jar=4318:/datahub/datahub-mce-consumer/scripts/prometheus-config.yaml "
fi

exec dockerize \
  $WAIT_FOR_KAFKA \
  $WAIT_FOR_SCHEMA_REGISTRY \
  -timeout 240s \
  java $JAVA_OPTS $JMX_OPTS $OTEL_AGENT $PROMETHEUS_AGENT -jar /datahub/datahub-mce-consumer/bin/mce-consumer-job.jar
