package com.datahub.event;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.PE_EVENT_CONSUMER_NAME;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datahub.event.hook.PlatformEventHook;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Conditional(PlatformEventProcessorCondition.class)
@EnableKafka
public class PlatformEventProcessor {

  private final OperationContext systemOperationContext;

  @Getter private final List<PlatformEventHook> hooks;
  private final Histogram kafkaLagStats =
      MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  @Autowired
  public PlatformEventProcessor(
      @Qualifier("systemOperationContext") @Nonnull final OperationContext systemOperationContext,
      List<PlatformEventHook> platformEventHooks) {
    log.info("Creating Platform Event Processor");
    this.systemOperationContext = systemOperationContext;
    this.hooks =
        platformEventHooks.stream()
            .filter(PlatformEventHook::isEnabled)
            .collect(Collectors.toList());
    log.info(
        "Enabled platform hooks: {}",
        this.hooks.stream()
            .map(hook -> hook.getClass().getSimpleName())
            .collect(Collectors.toList()));
    this.hooks.forEach(PlatformEventHook::init);
  }

  @KafkaListener(
      id = "${PLATFORM_EVENT_KAFKA_CONSUMER_GROUP_ID:generic-platform-event-job-client}",
      topics = {"${PLATFORM_EVENT_TOPIC_NAME:" + Topics.PLATFORM_EVENT + "}"},
      containerFactory = PE_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    try (Timer.Context i = MetricUtils.timer(this.getClass(), "consume").time()) {

      log.debug("Consuming a Platform Event");

      kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());
      final GenericRecord record = consumerRecord.value();
      log.info(
          "Got PE event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
          consumerRecord.key(),
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.serializedValueSize(),
          consumerRecord.timestamp());
      MetricUtils.counter(this.getClass(), "received_pe_count").inc();

      PlatformEvent event;
      try {
        event = EventUtils.avroToPegasusPE(record);
        log.debug("Successfully converted Avro PE to Pegasus PE. name: {}", event.getName());
      } catch (Exception e) {
        MetricUtils.counter(this.getClass(), "avro_to_pegasus_conversion_failure").inc();
        log.error("Error deserializing message due to: ", e);
        log.error("Message: {}", record.toString());
        return;
      }

      log.info("Invoking PE hooks for event name {}", event.getName());

      for (PlatformEventHook hook : this.hooks) {
        log.info(
            "Invoking PE hook {} for event name {}",
            hook.getClass().getSimpleName(),
            event.getName());
        try (Timer.Context ignored =
            MetricUtils.timer(this.getClass(), hook.getClass().getSimpleName() + "_latency")
                .time()) {
          hook.invoke(systemOperationContext, event);
        } catch (Exception e) {
          // Just skip this hook and continue.
          MetricUtils.counter(this.getClass(), hook.getClass().getSimpleName() + "_failure").inc();
          log.error(
              "Failed to execute PE hook with name {}", hook.getClass().getCanonicalName(), e);
        }
      }
      MetricUtils.counter(this.getClass(), "consumed_pe_count").inc();
      log.info("Successfully completed PE hooks for event with name {}", event.getName());
    }
  }
}
