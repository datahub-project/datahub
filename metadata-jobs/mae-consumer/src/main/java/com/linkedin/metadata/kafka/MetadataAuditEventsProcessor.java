package com.linkedin.metadata.kafka;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.kafka.config.MetadataChangeLogProcessorCondition;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.Topics;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@Conditional(MetadataChangeLogProcessorCondition.class)
@EnableKafka
public class MetadataAuditEventsProcessor {

  private final Histogram kafkaLagStats = MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  @Autowired
  public MetadataAuditEventsProcessor() {
  }

  @KafkaListener(id = "${METADATA_AUDIT_EVENT_KAFKA_CONSUMER_GROUP_ID:mae-consumer-job-client}", topics =
      "${KAFKA_TOPIC_NAME:" + Topics.METADATA_AUDIT_EVENT + "}", containerFactory = "kafkaEventConsumer")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());

    final GenericRecord record = consumerRecord.value();
    log.debug("Got MAE");

    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "maeProcess").time()) {
      final MetadataAuditEvent event = EventUtils.avroToPegasusMAE(record);

      final RecordTemplate snapshot = RecordUtils.getSelectedRecordTemplateFromUnion(event.getNewSnapshot());

      log.info(snapshot.toString());
    } catch (Exception e) {
      log.error("Error deserializing message: {}", e.toString());
      log.error("Message: {}", record.toString());
    }
  }
}
