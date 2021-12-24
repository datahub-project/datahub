# This "container" is a workaround to pre-create topics
FROM kymeric/cp-kafka:latest

ENV METADATA_AUDIT_EVENT_NAME="MetadataAuditEvent_v4"
ENV METADATA_CHANGE_EVENT_NAME="MetadataChangeEvent_v4"
ENV FAILED_METADATA_CHANGE_EVENT_NAME="FailedMetadataChangeEvent_v4"
ENV DATAHUB_USAGE_EVENT_NAME="DataHubUsageEvent_v1"
ENV METADATA_CHANGE_LOG_VERSIONED_TOPIC="MetadataChangeLog_Versioned_v1"
ENV METADATA_CHANGE_LOG_TIMESERIES_TOPIC="MetadataChangeLog_Timeseries_v1"
ENV METADATA_CHANGE_PROPOSAL_TOPIC="MetadataChangeProposal_v1"
ENV FAILED_METADATA_CHANGE_PROPOSAL_TOPIC="FailedMetadataChangeProposal_v1"

COPY ./kafka-setup.sh ./kafka-setup.sh

CMD ./kafka-setup.sh
