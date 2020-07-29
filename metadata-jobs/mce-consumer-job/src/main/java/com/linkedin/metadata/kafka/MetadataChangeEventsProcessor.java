package com.linkedin.metadata.kafka;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.internal.BaseRemoteWriterDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.FailedMetadataChangeEvent;
import com.linkedin.mxe.MetadataChangeEvent;
import com.linkedin.mxe.Topics;
import java.io.IOException;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@EnableKafka
public class MetadataChangeEventsProcessor {

  private BaseRemoteWriterDAO remoteWriterDAO;
  private KafkaTemplate<String, GenericRecord> kafkaTemplate;

  @Value("${KAFKA_FMCE_TOPIC_NAME:" + Topics.FAILED_METADATA_CHANGE_EVENT + "}")
  private String fmceTopicName;

  public MetadataChangeEventsProcessor(BaseRemoteWriterDAO remoteWriterDAO,
      KafkaTemplate<String, GenericRecord> kafkaTemplate) {
    this.remoteWriterDAO = remoteWriterDAO;
    this.kafkaTemplate = kafkaTemplate;
  }

  @KafkaListener(id = "mce-consumer-job-client",
      topics = "${KAFKA_MCE_TOPIC_NAME:" + Topics.METADATA_CHANGE_EVENT + "}")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    final GenericRecord record = consumerRecord.value();
    log.debug("Got MCE");
    log.debug("Record ", record);

    MetadataChangeEvent event = new MetadataChangeEvent();

    try {
      event = EventUtils.avroToPegasusMCE(record);
      log.debug("MetadataChangeEvent {}", event);
      if (event.hasProposedSnapshot()) {
        processProposedSnapshot(event);
      }
    } catch (Throwable throwable) {
      log.error("MCE Processor Error", throwable);
      log.error("Message: {}", record);
      sendFailedMCE(event, throwable);
    }
  }

  /**
   * Sending Failed MCE Event to Kafka Topic
   *
   * @param event
   * @param throwable
   */
  private void sendFailedMCE(@Nonnull MetadataChangeEvent event, @Nonnull Throwable throwable) {
    final FailedMetadataChangeEvent failedMetadataChangeEvent = createFailedMCEEvent(event, throwable);
    try {
      final GenericRecord genericFailedMCERecord = EventUtils.pegasusToAvroFailedMCE(failedMetadataChangeEvent);
      log.debug("Sending FailedMessages to topic - {}", fmceTopicName);
      log.info("Error while processing MCE: FailedMetadataChangeEvent - {}", failedMetadataChangeEvent);
      this.kafkaTemplate.send(fmceTopicName, genericFailedMCERecord);
    } catch (IOException e) {
      log.error("Error while sending FailedMetadataChangeEvent: Exception  - {}, FailedMetadataChangeEvent - {}",
          e.getStackTrace(), failedMetadataChangeEvent);
    }
  }

  /**
   * Populate a FailedMetadataChangeEvent from a MCE
   *
   * @param event
   * @param throwable
   * @return FailedMetadataChangeEvent
   */
  @Nonnull
  private FailedMetadataChangeEvent createFailedMCEEvent(@Nonnull MetadataChangeEvent event,
      @Nonnull Throwable throwable) {
    final FailedMetadataChangeEvent fmce = new FailedMetadataChangeEvent();
    fmce.setError(ExceptionUtils.getStackTrace(throwable));
    fmce.setMetadataChangeEvent(event);
    return fmce;
  }

  private void processProposedSnapshot(@Nonnull MetadataChangeEvent metadataChangeEvent) throws URISyntaxException {
    Snapshot snapshotUnion = metadataChangeEvent.getProposedSnapshot();
    final RecordTemplate snapshot = RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion);
    final Urn urn = ModelUtils.getUrnFromSnapshotUnion(snapshotUnion);
    remoteWriterDAO.create(urn, snapshot);
  }
}
