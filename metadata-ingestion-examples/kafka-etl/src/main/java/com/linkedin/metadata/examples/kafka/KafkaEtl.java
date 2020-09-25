package com.linkedin.metadata.examples.kafka;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.mxe.MetadataChangeEvent;
import com.linkedin.schema.KafkaSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Gathers Kafka topics from the local zookeeper instance and schemas from the schema registry, and then fires
 * MetadataChangeEvents for their schemas.
 *
 * <p>This should cause DataHub to be populated with this information, assuming it and the mce-consumer-job are running
 * locally.
 *
 * <p>Can be run with {@code ./gradlew :metadata-ingestion-examples:java:kafka-etl:bootRun}.
 */
@Slf4j
@Component
public final class KafkaEtl implements CommandLineRunner {
  private static final DataPlatformUrn KAFKA_URN = new DataPlatformUrn("kafka");

  @Inject
  @Named("kafkaEventProducer")
  private Producer<String, GenericRecord> _producer;

  @Inject
  @Named("zooKeeper")
  private ZooKeeper _zooKeeper;

  @Inject
  @Named("schemaRegistryClient")
  private SchemaRegistryClient _schemaRegistryClient;

  private SchemaMetadata buildDatasetSchema(String datasetName, String schema, int schemaVersion) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(new CorpuserUrn(System.getenv("USER")));
    final SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setKafkaSchema(new KafkaSchema().setDocumentSchema(schema));
    return new SchemaMetadata().setSchemaName(datasetName)
        .setPlatform(KAFKA_URN)
        .setCreated(auditStamp)
        .setLastModified(auditStamp)
        .setVersion(schemaVersion)
        .setHash("")
        .setPlatformSchema(platformSchema)
        .setFields(new SchemaFieldArray(new SchemaField().setFieldPath("")
            .setDescription("")
            .setNativeDataType("string")
            .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())))));
  }

  private void produceKafkaDatasetMce(SchemaMetadata schemaMetadata) {
    MetadataChangeEvent.class.getClassLoader().getResource("avro/com/linkedin/mxe/MetadataChangeEvent.avsc");

    // Kafka topics are considered datasets in the current DataHub metadata ecosystem.
    final KafkaMetadataEventProducer<DatasetSnapshot, DatasetAspect, DatasetUrn> eventProducer =
        new KafkaMetadataEventProducer<>(DatasetSnapshot.class, DatasetAspect.class, _producer);
    eventProducer.produceSnapshotBasedMetadataChangeEvent(
        new DatasetUrn(KAFKA_URN, schemaMetadata.getSchemaName(), FabricType.PROD), schemaMetadata);
    _producer.flush();
  }

  @Override
  public void run(String... args) throws Exception {
    log.info("Starting up");

    final List<String> topics = _zooKeeper.getChildren("/brokers/topics", false);
    for (String datasetName : topics) {
      if (datasetName.startsWith("_")) {
        continue;
      }

      final String topic = datasetName + "-value";
      io.confluent.kafka.schemaregistry.client.SchemaMetadata schemaMetadata;
      try {
        schemaMetadata = _schemaRegistryClient.getLatestSchemaMetadata(topic);
      } catch (Throwable t) {
        log.error("Failed to get schema for topic " + datasetName, t);
        log.error("Common failure: does this event schema exist in the schema registry?");
        continue;
      }

      if (schemaMetadata == null) {
        log.warn(String.format("Skipping topic without schema: %s", topic));
        continue;
      }
      log.trace(topic);

      produceKafkaDatasetMce(buildDatasetSchema(datasetName, schemaMetadata.getSchema(), schemaMetadata.getVersion()));
      log.info("Successfully fired MCE for " + datasetName);
    }
  }
}
