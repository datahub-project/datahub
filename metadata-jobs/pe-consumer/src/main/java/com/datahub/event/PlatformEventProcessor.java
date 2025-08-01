package com.datahub.event;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.PE_EVENT_CONSUMER_NAME;

import com.datahub.event.hook.PlatformEventHook;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Conditional(PlatformEventProcessorCondition.class)
@EnableKafka
public class PlatformEventProcessor {
  public static final String DATAHUB_PLATFORM_EVENT_CONSUMER_GROUP_VALUE =
      "${PLATFORM_EVENT_KAFKA_CONSUMER_GROUP_ID:generic-platform-event-job-client}";

  private final OperationContext systemOperationContext;

  @Getter private final List<PlatformEventHook> hooks;

  @Value(DATAHUB_PLATFORM_EVENT_CONSUMER_GROUP_VALUE)
  private String datahubPlatformEventConsumerGroupId;

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
      id = DATAHUB_PLATFORM_EVENT_CONSUMER_GROUP_VALUE,
      topics = {"${PLATFORM_EVENT_TOPIC_NAME:" + Topics.PLATFORM_EVENT + "}"},
      containerFactory = PE_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {

    systemOperationContext.withSpan(
        "consume",
        () -> {
          log.debug("Consuming a Platform Event");

          systemOperationContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils -> {
                    long queueTimeMs = System.currentTimeMillis() - consumerRecord.timestamp();

                    // Dropwizard legacy
                    metricUtils.histogram(this.getClass(), "kafkaLag", queueTimeMs);

                    // Micrometer with tags
                    // TODO: include priority level when available
                    metricUtils
                        .getRegistry()
                        .ifPresent(
                            meterRegistry -> {
                              meterRegistry
                                  .timer(
                                      MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
                                      "topic",
                                      consumerRecord.topic(),
                                      "consumer.group",
                                      datahubPlatformEventConsumerGroupId)
                                  .record(Duration.ofMillis(queueTimeMs));
                            });
                  });
          final GenericRecord record = consumerRecord.value();
          log.info(
              "Got PE event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
              consumerRecord.key(),
              consumerRecord.topic(),
              consumerRecord.partition(),
              consumerRecord.offset(),
              consumerRecord.serializedValueSize(),
              consumerRecord.timestamp());
          systemOperationContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils -> metricUtils.increment(this.getClass(), "received_pe_count", 1));

          PlatformEvent event;
          try {
            if (record == null) {
              log.error("Null record found. Dropping.");
              systemOperationContext
                  .getMetricUtils()
                  .ifPresent(
                      metricUtils -> metricUtils.increment(this.getClass(), "null_record", 1));
              return;
            }

            event = EventUtils.avroToPegasusPE(record);
            log.debug("Successfully converted Avro PE to Pegasus PE. name: {}", event.getName());
          } catch (Exception e) {
            systemOperationContext
                .getMetricUtils()
                .ifPresent(
                    metricUtils ->
                        metricUtils.increment(
                            this.getClass(), "avro_to_pegasus_conversion_failure", 1));
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

            systemOperationContext.withSpan(
                hook.getClass().getSimpleName(),
                () -> {
                  try {
                    hook.invoke(systemOperationContext, event);
                  } catch (Exception e) {
                    // Just skip this hook and continue.
                    systemOperationContext
                        .getMetricUtils()
                        .ifPresent(
                            metricUtils ->
                                metricUtils.increment(
                                    this.getClass(),
                                    hook.getClass().getSimpleName() + "_failure",
                                    1));
                    log.error(
                        "Failed to execute PE hook with name {}",
                        hook.getClass().getCanonicalName(),
                        e);
                  }
                },
                MetricUtils.DROPWIZARD_NAME,
                MetricUtils.name(this.getClass(), hook.getClass().getSimpleName() + "_latency"));
          }
          systemOperationContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils -> metricUtils.increment(this.getClass(), "consumed_pe_count", 1));
          log.info("Successfully completed PE hooks for event with name {}", event.getName());
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }
}
