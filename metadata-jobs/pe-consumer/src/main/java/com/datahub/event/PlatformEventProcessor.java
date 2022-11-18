package com.datahub.event;

import com.datahub.event.hook.PlatformEventHook;
import com.linkedin.gms.factory.kafka.KafkaEventConsumerFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.Topics;
import java.util.Collections;
import java.util.List;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.LongTaskTimer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@Conditional(PlatformEventProcessorCondition.class)
@Import({KafkaEventConsumerFactory.class})
@EnableKafka
public class PlatformEventProcessor {

  private final List<PlatformEventHook> hooks;
  private final DistributionSummary kafkaLagStats = MetricUtils.histogram(this.getClass(), "kafkaLag");

  @Autowired
  public PlatformEventProcessor() {
    log.info("Creating Platform Event Processor");
    this.hooks = Collections.emptyList(); // No event hooks (yet)
    this.hooks.forEach(PlatformEventHook::init);
  }

  @KafkaListener(id = "${PLATFORM_EVENT_KAFKA_CONSUMER_GROUP_ID:generic-platform-event-job-client}", topics = {
      "${PLATFORM_EVENT_TOPIC_NAME:" + Topics.PLATFORM_EVENT + "}" },
      containerFactory = "kafkaEventConsumer")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {

    log.info("Consuming a Platform Event");

    kafkaLagStats.record(System.currentTimeMillis() - consumerRecord.timestamp());
    final GenericRecord record = consumerRecord.value();
    log.debug("Got Generic PE on topic: {}, partition: {}, offset: {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
    MetricUtils.counter(this.getClass(), "received_pe_count").increment();

    PlatformEvent event;
    try {
      event = EventUtils.avroToPegasusPE(record);
      log.debug("Successfully converted Avro PE to Pegasus PE. name: {}",
          event.getName());
    } catch (Exception e) {
      MetricUtils.counter(this.getClass(), "avro_to_pegasus_conversion_failure").increment();
      log.error("Error deserializing message due to: ", e);
      log.error("Message: {}", record.toString());
      return;
    }

    log.debug("Invoking PE hooks for event name {}", event.getName());

    for (PlatformEventHook hook : this.hooks) {
      LongTaskTimer.Sample ignored = MetricUtils.timer(this.getClass(), hook.getClass().getSimpleName() + "_latency").start();
      try {
        hook.invoke(event);
      } catch (Exception e) {
        // Just skip this hook and continue.
        MetricUtils.counter(this.getClass(), hook.getClass().getSimpleName() + "_failure").increment();
        log.error("Failed to execute PE hook with name {}", hook.getClass().getCanonicalName(), e);
      }
      finally {
        ignored.stop();
      }
    }
    MetricUtils.counter(this.getClass(), "consumed_pe_count").increment();
    log.debug("Successfully completed PE hooks for event with name {}", event.getName());
  }
}
