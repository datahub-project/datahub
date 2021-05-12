# This "container" is a workaround to pre-create topics
FROM confluentinc/cp-kafka:5.4.0

ENV METADATA_AUDIT_EVENT_NAME="MetadataAuditEvent_v4"
ENV METADATA_CHANGE_EVENT_NAME="MetadataChangeEvent_v4"
ENV FAILED_METADATA_CHANGE_EVENT_NAME="FailedMetadataChangeEvent_v4"
ENV DATAHUB_USAGE_EVENT_NAME="DataHubUsageEvent_v1"

COPY ./kafka-setup.sh ./kafka-setup.sh

CMD ./kafka-setup.sh
