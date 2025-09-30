package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.*;
import static com.linkedin.metadata.utils.PegasusUtils.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
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
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
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

  // Constants for CDC JSON field names
  private static final String CDC_PAYLOAD_FIELD = "payload";
  private static final String CDC_BEFORE_FIELD = "before";
  private static final String CDC_AFTER_FIELD = "after";
  private static final String CDC_URN_FIELD = "urn";
  private static final String CDC_ASPECT_FIELD = "aspect";
  private static final String CDC_METADATA_FIELD = "metadata";
  private static final String CDC_SYSTEM_METADATA_FIELD = "systemmetadata";
  private static final String CDC_CREATED_ON_FIELD = "createdon";
  private static final String CDC_CREATED_BY_FIELD = "createdby";
  private static final String CDC_CREATED_FOR_FIELD = "createdfor";
  private static final String CDC_VERSION_FIELD = "version";

  // Constants for consumer configuration
  private static final String CDC_CONSUMER_GROUP_ID = "cdc-consumer-job-client";

  private final OperationContext systemOperationContext;

  private final EntityService entityService;

  @Qualifier("kafkaThrottle")
  private final ThrottleSensor kafkaThrottle;

  private final KafkaListenerEndpointRegistry registry;
  private final ConfigurationProvider provider;

  @Value("${mclProcessing.cdcSource.enabled:false}")
  @VisibleForTesting
  boolean cdcMclProcessingEnabled;

  @Value("${kafka.topics.cdcTopic.name:datahub.datahub.metadata_aspect_v2}")
  private String cdcTopicName;

  @Value("${kafka.consumer.cdcConsumerGroupId:cdc-consumer-job-client}")
  @VisibleForTesting
  String cdcConsumerGroupId;

  @PostConstruct
  public void registerConsumerThrottle() {
    if (cdcMclProcessingEnabled) {
      KafkaListenerUtil.registerThrottle(kafkaThrottle, provider, registry, cdcConsumerGroupId);
    }
  }

  @KafkaListener(
      id = CDC_CONSUMER_GROUP_ID,
      topics =
          "#{${mclProcessing.cdcSource.enabled:true}?'${kafka.topics.cdcTopic.name:datahub.datahub.metadata_aspect_v2}' : null}",
      containerFactory = CDC_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  public void consume(final ConsumerRecord<String, String> consumerRecord) {
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

  @VisibleForTesting
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

      // Step 1: Parse JSON using Jackson ObjectMapper
      JsonNode cdcRecord = systemOperationContext.getObjectMapper().readTree(record);
      log.debug("CDC Record {}", record);

      Optional<MetadataChangeLog> mcl = mclFromCDCRecord(cdcRecord);
      if (mcl.isPresent()) {
        entityService.produceMCLAsync(this.systemOperationContext, mcl.get());
        log.debug(
            "Successfully processed CDC record for urn: {}, aspect: {}",
            mcl.get().getEntityUrn(),
            mcl.get().getAspectName());
      }
    } catch (Exception e) {
      log.error("Error processing CDC record {} with error", record, e);
    }
  }

  @VisibleForTesting
  Optional<MetadataChangeLog> mclFromCDCRecord(JsonNode cdcRecord) throws URISyntaxException {
    // If json schema is turned off, the payload field is the message.
    JsonNode payload =
        cdcRecord.has(CDC_PAYLOAD_FIELD) ? cdcRecord.get(CDC_PAYLOAD_FIELD) : cdcRecord;

    // Step 2: Extract "before" and "after" fields as JsonNode objects
    JsonNode beforeRecord = payload.get(CDC_BEFORE_FIELD);
    JsonNode afterRecord = payload.get(CDC_AFTER_FIELD);

    // Step 3: Check if we should process this record (only latest versions)
    Pair<Boolean, ChangeType> recordState = shouldProcessCDCRecord(afterRecord, beforeRecord);
    if (!recordState.getFirst()) {
      return Optional.empty();
    }

    // Step 4: Extract urn, aspect, metadata, systemmetadata fields from both before/after CDC
    // records
    String urn = extractRequiredFieldWithFallback(afterRecord, beforeRecord, CDC_URN_FIELD);
    String aspectName =
        extractRequiredFieldWithFallback(afterRecord, beforeRecord, CDC_ASPECT_FIELD);

    String beforeMetadata = extractFieldFromRecord(beforeRecord, CDC_METADATA_FIELD);
    String afterMetadata = extractFieldFromRecord(afterRecord, CDC_METADATA_FIELD);

    String beforeSystemMetadata = extractFieldFromRecord(beforeRecord, CDC_SYSTEM_METADATA_FIELD);
    String afterSystemMetadata = extractFieldFromRecord(afterRecord, CDC_SYSTEM_METADATA_FIELD);

    JsonNode createdOn =
        extractRequiredFieldNodeWithFallback(afterRecord, beforeRecord, CDC_CREATED_ON_FIELD);
    long createdOnEpocMillis = extractTimestamp(createdOn);
    String createdBy =
        extractRequiredFieldWithFallback(afterRecord, beforeRecord, CDC_CREATED_BY_FIELD);
    String createdFor = extractFieldFromRecord(afterRecord, CDC_CREATED_FOR_FIELD);

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
    auditStamp.setTime(createdOnEpocMillis);
    auditStamp.setActor(Urn.createFromString(createdBy));
    if (createdFor != null) {
      auditStamp.setImpersonator(Urn.createFromString(createdFor));
    }

    ChangeType changeType = recordState.getSecond();
    if (changeType.equals(ChangeType.UPSERT)) {
      if (SystemMetadataUtils.isNoOp(newSystemMetadata) || Objects.equals(oldValue, newValue)) {
        changeType = ChangeType.RESTATE;
      }
    }

    if (changeType.equals(ChangeType.DELETE)) {
      // MCLs for DELETE contain current value in newValue for DELETE Records. TODO: Confirm
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

  @VisibleForTesting
  String extractFieldFromRecord(JsonNode record, String fieldName) {
    return record != null
            && !record.isNull()
            && record.has(fieldName)
            && !record.get(fieldName).isNull()
            && !record.get(fieldName).asText().isEmpty()
        ? record.get(fieldName).asText()
        : null;
  }

  @VisibleForTesting
  String extractRequiredFieldWithFallback(
      JsonNode afterRecord, JsonNode beforeRecord, String fieldName) {
    return extractRequiredFieldNodeWithFallback(afterRecord, beforeRecord, fieldName).asText();
  }

  JsonNode extractRequiredFieldNodeWithFallback(
      JsonNode afterRecord, JsonNode beforeRecord, String fieldName) {
    return afterRecord != null && !afterRecord.isNull()
        ? afterRecord.get(fieldName)
        : beforeRecord.get(fieldName);
  }

  @VisibleForTesting
  Pair<Boolean, ChangeType> shouldProcessCDCRecord(JsonNode afterRecord, JsonNode beforeRecord) {
    if ((afterRecord != null && !afterRecord.isNull() && afterRecord.has(CDC_VERSION_FIELD))
        && afterRecord.get(CDC_VERSION_FIELD).asLong() == 0) {
      return Pair.of(true, ChangeType.UPSERT);
    }
    if ((afterRecord == null || afterRecord.isNull() || !afterRecord.has(CDC_VERSION_FIELD))
        && beforeRecord != null
        && !beforeRecord.isNull()
        && beforeRecord.has(CDC_VERSION_FIELD)
        && beforeRecord.get(CDC_VERSION_FIELD).asLong() == 0) {
      return Pair.of(true, ChangeType.DELETE); // This is a delete
    }
    return Pair.of(false, ChangeType.$UNKNOWN);
  }

  /**
   * Extracts timestamp from createdOn JsonNode, handling two scenarios: 1. MySQL format: Long value
   * representing epoch microseconds 2. PostgreSQL format: ISO timestamp string with timezone offset
   *
   * @param createdOnValue The createdOn field value as JsonNode
   * @return Epoch milliseconds
   */
  @VisibleForTesting
  long extractTimestamp(@Nonnull JsonNode createdOnValue) {
    // Scenario 1: MySQL format - Long value (epoch microseconds)
    if (createdOnValue.isNumber()) {
      return createdOnValue.asLong() / 1000L; // Convert microseconds to milliseconds
    }

    // Scenario 2: PostgreSQL format - ISO timestamp string with timezone
    try {
      String postgresTimestamp = createdOnValue.asText();
      OffsetDateTime odt =
          OffsetDateTime.parse(postgresTimestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
      return odt.toInstant().toEpochMilli();
    } catch (DateTimeParseException ex) {
      log.error("Failed to parse timestamp '{}' as PostgreSQL format", createdOnValue.asText(), ex);
      throw new RuntimeException(ex.getMessage());
    }
  }
}
