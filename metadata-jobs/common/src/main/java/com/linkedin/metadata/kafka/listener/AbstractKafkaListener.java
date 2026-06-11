package com.linkedin.metadata.kafka.listener;

import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.kafka.context.inbound.InboundContextResolver;
import com.linkedin.metadata.utils.metrics.CascadeOperationContext;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

@Slf4j
public abstract class AbstractKafkaListener<E, H extends EventHook<E>, R>
    implements GenericKafkaListener<E, H, R> {

  @Getter protected OperationContext systemOperationContext;

  @Getter protected InboundContextResolver inboundContextResolver;

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
      @Nonnull Map<String, Set<String>> aspectsToDrop,
      @Nonnull InboundContextResolver inboundContextResolver) {

    this.systemOperationContext = systemOperationContext;
    this.inboundContextResolver = inboundContextResolver;
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
  public void consumeEnvelope(@Nonnull final InboundMetadataEnvelope<R> envelope) {
    final OperationContext eventContext =
        inboundContextResolver.resolve(envelope, systemOperationContext);
    try {
      systemOperationContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  MetricUtils.recordInboundMessageQueueLag(
                      metricUtils,
                      this.getClass(),
                      envelope.getLogicalTopic(),
                      consumerGroupId,
                      envelope.getEnqueuedAtMillis(),
                      envelope.getMessagingSystem(),
                      envelope.getPriority()));
      log.debug(
          "Got event consumer: {} key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
          consumerGroupId,
          envelope.getKey(),
          envelope.getLogicalTopic(),
          envelope.getKafkaPartition(),
          envelope.getKafkaOffset(),
          envelope.getSerializedValueSize(),
          envelope.getEnqueuedAtMillis());

      handleInbound(envelope.getPayload(), envelope.getLogicalTopic(), eventContext);
    } finally {
      MDC.clear();
    }
  }

  /**
   * Shared inbound processing path used by both transports after the per-event {@link
   * OperationContext} has been resolved and transport-specific intake metrics / log lines have been
   * recorded.
   */
  private void handleInbound(
      @Nonnull final R rawPayload,
      @Nonnull final String topic,
      @Nonnull final OperationContext eventContext) {
    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metricUtils ->
                metricUtils.increment(
                    this.getClass(), consumerGroupId + "_received_event_count", 1));

    E event;
    try {
      event = convertRecord(rawPayload);
    } catch (Exception e) {
      systemOperationContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(
                      this.getClass(), consumerGroupId + "_conversion_failure", 1));
      log.error("Error deserializing message due to: ", e);
      log.error("Message: {}", rawPayload);
      return;
    }

    // Initialize MDC context with event metadata
    setMDCContext(event);

    // Propagate cascade operation ID from SystemMetadata to MDC for cross-service correlation
    SystemMetadata sysMetadata = getSystemMetadata(event);
    if (sysMetadata != null && sysMetadata.getProperties() != null) {
      String cascadeOpId =
          sysMetadata.getProperties().get(CascadeOperationContext.SYSTEM_METADATA_CASCADE_ID_KEY);
      if (cascadeOpId != null) {
        MDC.put(CascadeOperationContext.MDC_CASCADE_OPERATION_ID, cascadeOpId);
      }
    }

    // Check if should skip processing
    if (shouldSkipProcessing(event)) {
      log.info("Skipping event: {}", event);
      return;
    }

    List<String> loggingAttributes = getFineGrainedLoggingAttributes(event);

    processWithHooks(event, loggingAttributes, topic, eventContext);
  }

  /**
   * Process the event with all registered hooks.
   *
   * @param event The event to process
   * @param loggingAttributes Attributes for logging
   * @param topic logical topic of the inbound message
   * @param eventContext per-event OperationContext threaded from {@link InboundContextResolver};
   *     used for hook invocation. The surrounding span / metric calls still use the system context.
   */
  protected void processWithHooks(
      E event,
      List<String> loggingAttributes,
      String topic,
      @Nonnull OperationContext eventContext) {
    systemOperationContext.withQueueSpan(
        "consume",
        getSystemMetadata(event),
        topic,
        () -> {
          log.debug(
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
                    hook.invoke(eventContext, event);
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
          log.debug(
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
