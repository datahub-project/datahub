package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.*;
import static com.linkedin.metadata.utils.PegasusUtils.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.gms.factory.kafka.SimpleKafkaConsumerFactory;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.kafka.config.CDCProcessorCondition;
import com.linkedin.metadata.kafka.util.KafkaListenerUtil;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableKafka
@Conditional(CDCProcessorCondition.class)
@Import({
  RestliEntityClientFactory.class,
  SimpleKafkaConsumerFactory.class,
  com.linkedin.gms.factory.kafka.CDCConsumerFactory.class
})
@RequiredArgsConstructor
public class CDCProcessor {
  private final OperationContext systemOperationContext;

  private final EntityService entityService;

  @Qualifier("kafkaThrottle")
  private final ThrottleSensor kafkaThrottle;

  private final KafkaListenerEndpointRegistry registry;
  private final ConfigurationProvider provider;

  @Value("${mclProcessing.cdcSource.enabled:false}")
  private boolean cdcMclProcessingEnabled;

  @Value("${kafka.topic.cdcTopic.name:datahub.datahub.metadata_aspect_v2}")
  private String cdcTopicName;

  private String cdcConsumerGroupId = "cdc-consumer-job-client"; // TODO: Use env var for overrides

  @PostConstruct
  public void registerConsumerThrottle() {
    if (cdcMclProcessingEnabled) {
      KafkaListenerUtil.registerThrottle(kafkaThrottle, provider, registry, cdcConsumerGroupId);
    }
  }

  @KafkaListener(
      id = "cdc-consumer-job-client",
      topics =
          "#{${mclProcessing.cdcSource.enabled:true}?'${kafka.topic.cdcTopic.name:datahub.datahub.metadata_aspect_v2}' : null}",
      containerFactory = CDC_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  public void consume(final ConsumerRecord<String, String> consumerRecord) {
    if (!cdcMclProcessingEnabled) {
      log.warn("CDC processing is disabled but consumer received message. This should not happen.");
      return;
    }

    try {

      systemOperationContext
          .getMetricUtils()
          .ifPresent(
              metricUtils -> {
                long queueTimeMs = System.currentTimeMillis() - consumerRecord.timestamp();

                // Dropwizard legacy
                metricUtils.histogram(this.getClass(), "kafkaLag", queueTimeMs);

                // Micrometer with tags
                metricUtils
                    .getRegistry()
                    .timer(
                        MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
                        "topic",
                        consumerRecord.topic(),
                        "consumer.group",
                        cdcConsumerGroupId)
                    .record(Duration.ofMillis(queueTimeMs));
              });

      final String record = consumerRecord.value();

      log.info(
          "Got CDC event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
          consumerRecord.key(),
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.serializedValueSize(),
          consumerRecord.timestamp());

      // if (log.isDebugEnabled()) {
      log.info("CDC Record {}", record);
      if (record != null) {
        processCDCRecord(record);
      }

    } catch (Exception e) {
      log.error("CDC Processor Error", e);
      log.error("CDC Message: {}", consumerRecord.value());
    } finally {
      MDC.clear();
    }
  }

  @com.google.common.annotations.VisibleForTesting
  void processCDCRecord(final String record) {
    try {

      // Parse the Debezium CDC JSON record containing before/after values
      // The record structure follows Debezium format:
      // {
      //   "before": { "urn": "...", "aspect": "...", "version": N, "metadata": "{...}", ... },
      //   "after": { "urn": "...", "aspect": "...", "version": N, "metadata": "{...}", ... },
      //   "op": "c|u|d", // create/update/delete
      //   "ts_ms": timestamp,
      //   "source": { ... }
      // }

      // 1. Parse JSON using Jackson ObjectMapper (available via
      // systemOperationContext.getObjectMapper())
      // 2. Extract "before" and "after" fields as JsonNode objects
      // 3. From "after" field, extract version - if version != 0, discard (only process latest
      // versions)
      // 4. Extract urn, aspect, metadata, systemmetadata fields from both before/after CDC records
      //    - CDC before/after contain complete database rows: {urn, aspect, version, metadata,
      // systemmetadata, createdon, createdby, createdfor}
      //    - The "metadata" field contains the serialized JSON of the actual aspect RecordTemplate
      //    - The "systemmetadata" field contains DataHub system metadata JSON
      // 5. Deserialize the "metadata" JSON strings to RecordTemplate objects:
      //    - Use RecordUtils.toDataMap(before.metadata) and RecordUtils.toDataMap(after.metadata)
      //    - Convert DataMap to appropriate aspect RecordTemplate using entity registry and aspect
      // name
      //    - These RecordTemplate objects become oldValue/newValue for UpdateAspectResult
      // 6. Parse systemmetadata JSON strings to SystemMetadata objects for
      // oldSystemMetadata/newSystemMetadata
      // 7. Create ChangeMCP using "after" record data:
      //    - urn: from after.urn
      //    - aspectName: from after.aspect
      //    - changeType: UPSERT
      //    - recordTemplate: RecordTemplate parsed from after.metadata (step 5)
      //    - systemMetadata: SystemMetadata parsed from after.systemmetadata (step 6)
      //    - auditStamp: construct from after.createdOn (time), after.createdBy (actor),
      // after.createdFor (impersonator)
      // 8. Construct UpdateAspectResult with:
      //    - urn: from step 7
      //    - request: ChangeMCP from step 7
      //    - oldValue: RecordTemplate from before.metadata (step 5)
      //    - newValue: RecordTemplate from after.metadata (step 5)
      //    - oldSystemMetadata: from before.systemmetadata (step 6)
      //    - newSystemMetadata: from after.systemmetadata (step 6)
      //    - auditStamp: from step 7
      //
      //    - UpdateAspectResult.toMCL() generates MetadataChangeLog for downstream processing
      //    - ChangeMCP interface requires urn, aspectName, changeType, recordTemplate,
      // systemMetadata

      // Step 1: Parse JSON using Jackson ObjectMapper
      JsonNode cdcRecord = systemOperationContext.getObjectMapper().readTree(record);

      Optional<MetadataChangeLog> mcl = mclFromCDCRecord(cdcRecord);
      if (mcl.isPresent()) {
        entityService.produceMCLAsync(this.systemOperationContext, mcl.get());
        log.debug(
            "Successfully processed CDC record for urn: {}, aspect: {}",
            mcl.get().getEntityUrn(),
            mcl.get().getAspectName());
      }
    } catch (Exception e) {
      log.error("Error processing CDC record", e);
    }
  }

  @com.google.common.annotations.VisibleForTesting
  Optional<MetadataChangeLog> mclFromCDCRecord(JsonNode cdcRecord) throws URISyntaxException {
    JsonNode payload = cdcRecord.get("payload");
    // Step 2: Extract "before" and "after" fields as JsonNode objects
    JsonNode beforeRecord = payload.get("before");
    JsonNode afterRecord = payload.get("after");

    // Step 3: Check if we should process this record (only latest versions)
    Pair<Boolean, ChangeType> recordState = shouldProcessCDCRecord(afterRecord, beforeRecord);
    if (!recordState.getFirst()) {
      return Optional.empty();
    }

    // Step 4: Extract urn, aspect, metadata, systemmetadata fields from both before/after CDC
    // records
    String urn =
        afterRecord != null && !afterRecord.isNull()
            ? afterRecord.get("urn").asText()
            : beforeRecord.get("urn").asText();
    String aspectName =
        afterRecord != null && !afterRecord.isNull()
            ? afterRecord.get("aspect").asText()
            : beforeRecord.get("aspect").asText();

    String beforeMetadata =
        beforeRecord != null && !beforeRecord.isNull() && beforeRecord.has("metadata")
            ? beforeRecord.get("metadata").asText()
            : null;
    String afterMetadata =
        afterRecord != null && !afterRecord.isNull() && afterRecord.has("metadata")
            ? afterRecord.get("metadata").asText()
            : null;

    String beforeSystemMetadata =
        beforeRecord != null && !beforeRecord.isNull() && beforeRecord.has("systemmetadata")
            ? beforeRecord.get("systemmetadata").asText()
            : null;
    String afterSystemMetadata =
        afterRecord != null && !afterRecord.isNull() && afterRecord.has("systemmetadata")
            ? afterRecord.get("systemmetadata").asText()
            : null;

    String createdOn =
        afterRecord != null && !afterRecord.isNull()
            ? afterRecord.get("createdon").asText()
            : beforeRecord.get("createdon").asText();
    String createdBy =
        afterRecord != null && !afterRecord.isNull()
            ? afterRecord.get("createdby").asText()
            : beforeRecord.get("createdby").asText();
    String createdFor =
        afterRecord != null
                && !afterRecord.isNull()
                && afterRecord.has("createdfor")
                && !afterRecord.get("createdfor").isNull()
            ? afterRecord.get("createdfor").asText()
            : null;

    // Step 5: Deserialize the "metadata" JSON strings to RecordTemplate objects
    Urn entityUrn = Urn.createFromString(urn);
    String entityType = entityUrn.getEntityType();
    AspectSpec aspectSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(entityType)
            .getAspectSpec(aspectName);

    RecordTemplate oldValue = null;
    if (beforeMetadata != null) {
      DataMap beforeDataMap = RecordUtils.toDataMap(beforeMetadata);
      oldValue = RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(), beforeDataMap);
    }

    RecordTemplate newValue = null;
    if (afterMetadata != null) {
      DataMap afterDataMap = RecordUtils.toDataMap(afterMetadata);
      newValue = RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(), afterDataMap);
    }

    // Step 6: Parse systemmetadata JSON strings to SystemMetadata objects
    SystemMetadata oldSystemMetadata = null;
    if (beforeSystemMetadata != null) {
      oldSystemMetadata = RecordUtils.toRecordTemplate(SystemMetadata.class, beforeSystemMetadata);
    }

    SystemMetadata newSystemMetadata = null;
    if (afterSystemMetadata != null) {
      newSystemMetadata = RecordUtils.toRecordTemplate(SystemMetadata.class, afterSystemMetadata);
    }

    // Step 7: Create ChangeMCP using "after" record data
    AuditStamp auditStamp = new AuditStamp();
    // Debezium CDC json has represents timestamp (DATETIME(6)  as microseconds.
    auditStamp.setTime(Long.parseLong(createdOn) / 1000);
    auditStamp.setActor(Urn.createFromString(createdBy));
    if (createdFor != null) {
      auditStamp.setImpersonator(Urn.createFromString(createdFor));
    }

    boolean isKeyAspect = systemOperationContext.getKeyAspectName(entityUrn).equals(aspectName);
    ChangeType changeType = recordState.getSecond();
    // if (changeType.equals(ChangeType.DELETE) && !isKeyAspect){
    //  changeType = ChangeType.UPSERT; // Only the Key aspect delete can be a DELETE.
    // }
    // else
    if (changeType.equals(ChangeType.UPSERT)) {
      if (SystemMetadataUtils.isNoOp(newSystemMetadata) || Objects.equals(oldValue, newValue)) {
        changeType = ChangeType.RESTATE;
      }
    }

    if (changeType.equals(ChangeType.DELETE)) {
      RecordTemplate temp = newValue;
      newValue = oldValue;
      oldValue = temp;

      SystemMetadata tempMetadata = newSystemMetadata;
      newSystemMetadata = oldSystemMetadata;
      oldSystemMetadata = tempMetadata;
    }

    MetadataChangeLog mcl =
        constructMCL(
            null,
            urnToEntityName(entityUrn),
            entityUrn,
            changeType,
            aspectName,
            auditStamp,
            newValue,
            newSystemMetadata,
            oldValue,
            oldSystemMetadata);
    return Optional.of(mcl);
  }

  @com.google.common.annotations.VisibleForTesting
  Pair<Boolean, ChangeType> shouldProcessCDCRecord(JsonNode afterRecord, JsonNode beforeRecord) {
    if ((afterRecord != null && !afterRecord.isNull() && afterRecord.has("version"))
        && afterRecord.get("version").asLong() == 0) {
      return Pair.of(true, ChangeType.UPSERT);
    }
    if ((afterRecord == null || afterRecord.isNull() || !afterRecord.has("version"))
        && beforeRecord != null
        && !beforeRecord.isNull()
        && beforeRecord.has("version")
        && beforeRecord.get("version").asLong() == 0) {
      return Pair.of(true, ChangeType.DELETE); // This is a delete
    }
    return Pair.of(false, ChangeType.$UNKNOWN);
  }
}
