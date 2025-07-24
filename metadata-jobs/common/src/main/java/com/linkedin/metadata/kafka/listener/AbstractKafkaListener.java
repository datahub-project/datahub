package com.linkedin.metadata.kafka.listener;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

@Slf4j
public abstract class AbstractKafkaListener<E, H extends EventHook<E>, R>
    implements GenericKafkaListener<E, H, R> {

  protected OperationContext systemOperationContext;

  @Getter protected String consumerGroupId;

  @Getter protected List<H> hooks;

  protected boolean fineGrainedLoggingEnabled;
  protected Map<String, Set<String>> aspectsToDrop;

  @Override
  public GenericKafkaListener<E, H, R> init(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull String consumerGroup,
      @Nonnull List<H> hooks,
      boolean fineGrainedLoggingEnabled,
      @Nonnull Map<String, Set<String>> aspectsToDrop) {

    this.systemOperationContext = systemOperationContext;
    this.consumerGroupId = consumerGroup;
    this.hooks = hooks;
    this.hooks.forEach(hook -> hook.init(systemOperationContext));
    this.fineGrainedLoggingEnabled = fineGrainedLoggingEnabled;
    this.aspectsToDrop = aspectsToDrop;

    log.info(
        "Enabled Hooks - Group: {} Hooks: {}",
        consumerGroup,
        hooks.stream().map(hook -> hook.getClass().getSimpleName()).collect(Collectors.toList()));

    return this;
  }

  @Override
  public void consume(@Nonnull final ConsumerRecord<String, R> consumerRecord) {
    try {
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
                                  consumerGroupId)
                              .record(Duration.ofMillis(queueTimeMs));
                        });
              });
      final R record = consumerRecord.value();
      log.debug(
          "Got event consumer: {} key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
          consumerGroupId,
          consumerRecord.key(),
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.serializedValueSize(),
          consumerRecord.timestamp());

      systemOperationContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(
                      this.getClass(), consumerGroupId + "_received_event_count", 1));

      E event;
      try {
        event = convertRecord(record);
      } catch (Exception e) {
        systemOperationContext
            .getMetricUtils()
            .ifPresent(
                metricUtils ->
                    metricUtils.increment(
                        this.getClass(), consumerGroupId + "_conversion_failure", 1));
        log.error("Error deserializing message due to: ", e);
        log.error("Message: {}", record.toString());
        return;
      }

      // Initialize MDC context with event metadata
      setMDCContext(event);

      // Check if should skip processing
      if (shouldSkipProcessing(event)) {
        log.info("Skipping event: {}", event);
        return;
      }

      List<String> loggingAttributes = getFineGrainedLoggingAttributes(event);

      processWithHooks(event, loggingAttributes, consumerRecord.topic());

    } finally {
      MDC.clear();
    }
  }

  /**
   * Process the event with all registered hooks.
   *
   * @param event The event to process
   * @param loggingAttributes Attributes for logging
   */
  protected void processWithHooks(E event, List<String> loggingAttributes, String topic) {
    systemOperationContext.withQueueSpan(
        "consume",
        getSystemMetadata(event),
        topic,
        () -> {
          log.info(
              "Invoking hooks for consumer: {} event: {}",
              consumerGroupId,
              getEventDisplayString(event));

          // Process with each hook
          for (H hook : this.hooks) {
            final String hookName = hook.getClass().getSimpleName();

            systemOperationContext.withSpan(
                hookName,
                () -> {
                  log.debug(
                      "Invoking hook {} for event: {}", hookName, getEventDisplayString(event));
                  try {
                    hook.invoke(event);
                    updateMetrics(hookName, event);
                  } catch (Exception e) {
                    // Just skip this hook and continue - "at most once" processing
                    systemOperationContext
                        .getMetricUtils()
                        .ifPresent(
                            metricUtils ->
                                metricUtils.increment(this.getClass(), hookName + "_failure", 1));
                    log.error(
                        "Failed to execute hook with name {}",
                        hook.getClass().getCanonicalName(),
                        e);

                    Span currentSpan = Span.current();
                    currentSpan.recordException(e);
                    currentSpan.setStatus(StatusCode.ERROR, e.getMessage());
                    currentSpan.setAttribute(MetricUtils.ERROR_TYPE, e.getClass().getName());
                  }
                },
                Stream.concat(
                        Stream.of(
                            MetricUtils.DROPWIZARD_NAME,
                            MetricUtils.name(this.getClass(), hookName + "_latency")),
                        loggingAttributes.stream())
                    .toArray(String[]::new));
          }

          systemOperationContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils ->
                      metricUtils.increment(
                          this.getClass(), consumerGroupId + "_consumed_event_count", 1));
          log.info(
              "Successfully completed hooks for consumer: {} event: {}",
              consumerGroupId,
              getEventDisplayString(event));
        },
        Stream.concat(
                Stream.of(
                    MetricUtils.DROPWIZARD_NAME, MetricUtils.name(this.getClass(), "consume")),
                loggingAttributes.stream())
            .toArray(String[]::new));
  }

  /**
   * Sets MDC context based on event metadata.
   *
   * @param event The event to extract metadata from
   */
  protected abstract void setMDCContext(E event);

  /**
   * Determines if this event should be skipped based on filtering rules.
   *
   * @param event The event to check
   * @return true if event should be skipped, false otherwise
   */
  protected abstract boolean shouldSkipProcessing(E event);

  /**
   * Gets attributes for fine-grained logging.
   *
   * @param event The event to extract attributes from
   * @return List of attribute name-value pairs
   */
  protected abstract List<String> getFineGrainedLoggingAttributes(E event);

  /**
   * Gets system metadata from the event for tracing.
   *
   * @param event The event
   * @return System metadata object
   */
  protected abstract SystemMetadata getSystemMetadata(E event);

  /**
   * Gets a display string for the event for logging.
   *
   * @param event The event
   * @return Display string
   */
  protected abstract String getEventDisplayString(E event);

  /**
   * Optionally update metrics
   *
   * @param hookName name of the hook
   * @param event the event processed by the hook
   */
  protected void updateMetrics(String hookName, E event) {}
}
