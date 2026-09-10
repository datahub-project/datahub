package com.linkedin.metadata.kafka.usage;

import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.usage.UsageDedupHeaders;
import com.linkedin.metadata.usage.store.UsageAggregationStore;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.AgentClass;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.metadata.context.usage.UsageOperation;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Records {@code metadata_ingest} for MCPs consumed from Kafka/pgQueue when not pre-billed at GMS.
 */
@Slf4j
@RequiredArgsConstructor
public class UsageQueueIngestRecorder {

  private final UsageAggregationStore usageStore;

  public void recordIfNeeded(
      @Nonnull InboundMetadataEnvelope<?> envelope,
      @Nonnull OperationContext eventContext,
      @Nonnull MetadataChangeProposal mcp) {
    if (UsageDedupHeaders.isPreRecorded(mcp)) {
      return;
    }
    try {
      String actorUrn = resolveActorUrn(mcp, eventContext);
      RequestContext.RequestContextBuilder requestBuilder =
          RequestContext.builder()
              .actorUrn(actorUrn)
              .sourceIP("queue")
              .requestAPI(RequestContext.RequestAPI.MESSAGING)
              .requestID(buildRequestId(envelope))
              .userAgent("datahub/mce-consumer")
              .agentClass(AgentClass.INGESTION)
              .usageIdentity(actorUrn)
              .authChannel(AuthChannel.UNKNOWN)
              .withUsageOperation(UsageOperation.METADATA_INGEST)
              .withUsageQuantity(1);
      if (envelope.getSerializedValueSize() != null && envelope.getSerializedValueSize() > 0) {
        requestBuilder.inputBytes(envelope.getSerializedValueSize().longValue());
        requestBuilder.requestBodyMaterialized(true);
      }
      RequestContext requestContext = requestBuilder.build();
      OperationContext usageContext =
          eventContext.toBuilder()
              .requestContext(requestContext)
              .build(eventContext.getSessionAuthentication(), true);
      usageStore.recordRequest(usageContext);
    } catch (RuntimeException e) {
      log.warn("Failed to record queue-path metadata_ingest usage", e);
    }
  }

  @Nonnull
  private static String buildRequestId(@Nonnull InboundMetadataEnvelope<?> envelope) {
    if (envelope.getKafkaPartition() != null && envelope.getKafkaOffset() != null) {
      return String.format(
          "mcp:%s:%d:%d",
          envelope.getLogicalTopic(), envelope.getKafkaPartition(), envelope.getKafkaOffset());
    }
    if (envelope.getEnqueueSeq() != null) {
      return String.format("mcp:%s:%d", envelope.getLogicalTopic(), envelope.getEnqueueSeq());
    }
    return "mcp:" + envelope.getLogicalTopic();
  }

  @Nonnull
  private static String resolveActorUrn(
      @Nonnull MetadataChangeProposal mcp, @Nonnull OperationContext eventContext) {
    String fromMetadata = resolveActorFromSystemMetadata(mcp.getSystemMetadata());
    if (fromMetadata != null) {
      return fromMetadata;
    }
    if (eventContext.getSessionAuthentication().getActor() != null) {
      return eventContext.getSessionAuthentication().getActor().toUrnStr();
    }
    return Constants.SYSTEM_ACTOR;
  }

  @Nullable
  private static String resolveActorFromSystemMetadata(@Nullable SystemMetadata systemMetadata) {
    if (systemMetadata == null || !systemMetadata.hasAspectModified()) {
      return null;
    }
    AuditStamp aspectModified = systemMetadata.getAspectModified();
    if (!aspectModified.hasActor()) {
      return null;
    }
    String actor = aspectModified.getActor().toString();
    if (Constants.SYSTEM_ACTOR.equals(actor)) {
      return null;
    }
    return actor;
  }
}
