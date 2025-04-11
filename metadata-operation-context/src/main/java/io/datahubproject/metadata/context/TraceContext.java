package io.datahubproject.metadata.context;

import static com.linkedin.metadata.utils.metrics.MetricUtils.BATCH_SIZE_ATTR;
import static com.linkedin.metadata.utils.metrics.MetricUtils.QUEUE_DURATION_MS_ATTR;
import static com.linkedin.metadata.utils.metrics.MetricUtils.QUEUE_ENQUEUED_AT_ATTR;

import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder
public class TraceContext implements ContextInterface {
  // trace logging
  public static final String TRACE_HEADER = "X-Enable-Trace-Log";
  public static final String TRACE_COOKIE = "enable-trace-log";
  // system metadata properties
  public static final String TELEMETRY_TRACE_KEY = "telemetryTraceId";
  public static final String TELEMETRY_QUEUE_SPAN_KEY = "telemetryQueueSpanId";
  public static final String TELEMETRY_LOG_KEY = "telemetryLog";
  public static final String TELEMETRY_ENQUEUED_AT = "telemetryEnqueuedAt";

  public static final TraceIdGenerator TRACE_ID_GENERATOR = new TraceIdGenerator();
  public static final SpanExporter LOG_SPAN_EXPORTER = new ConditionalLogSpanExporter();

  private static final ThreadLocal<Boolean> logTracingEnabled = new ThreadLocal<>();

  public static boolean isLogTracingEnabled() {
    Boolean enabled = logTracingEnabled.get();
    return enabled != null && enabled;
  }

  public static void clear() {
    logTracingEnabled.remove();
  }

  public static void enableLogTracing(HttpServletRequest request) {
    // Check header
    String headerValue = request.getHeader(TRACE_HEADER);
    if ("true".equalsIgnoreCase(headerValue)) {
      logTracingEnabled.set(true);
      return;
    }

    // Check cookie
    Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (TRACE_COOKIE.equals(cookie.getName()) && "true".equalsIgnoreCase(cookie.getValue())) {
          logTracingEnabled.set(true);
          return;
        }
      }
    }

    logTracingEnabled.set(false);
  }

  @Getter @Nonnull private final Tracer tracer;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }

  /**
   * Generic method to capture spans
   *
   * @param name name of the span
   * @param operation the actual logic
   * @param attributes additional attributes
   * @return the output from the logic
   * @param <T> generic
   */
  public <T> T withSpan(String name, Supplier<T> operation, String... attributes) {
    Span span = tracer.spanBuilder(name).startSpan();
    try (var scope = span.makeCurrent()) {
      for (int i = 0; i < attributes.length; i += 2) {
        span.setAttribute(attributes[i], attributes[i + 1]);
      }
      return operation.get();
    } catch (Exception e) {
      span.setStatus(StatusCode.ERROR, e.getMessage());
      span.recordException(e);
      span.setAttribute(MetricUtils.ERROR_TYPE, e.getClass().getName());
      throw e;
    } finally {
      span.end();
    }
  }

  protected void withSpan(String name, Runnable operation, String... attributes) {
    Span span = tracer.spanBuilder(name).startSpan();
    try (var scope = span.makeCurrent()) {
      for (int i = 0; i < attributes.length; i += 2) {
        span.setAttribute(attributes[i], attributes[i + 1]);
      }
      operation.run();
    } catch (Exception e) {
      span.setStatus(StatusCode.ERROR, e.getMessage());
      span.recordException(e);
      span.setAttribute(MetricUtils.ERROR_TYPE, e.getClass().getName());
      throw e;
    } finally {
      span.end();
    }
  }

  /**
   * Handle multiple messages with different trace ids processed from a queue
   *
   * @param name name of the processing of the queue
   * @param batchSystemMetadata batch of system metadata
   * @param operation actual processing logic
   * @param attributes span attributes
   */
  protected void withQueueSpan(
      String name,
      List<SystemMetadata> batchSystemMetadata,
      String topicName,
      Runnable operation,
      String... attributes) {

    List<SystemMetadata> tracingEnabledSystemMetadata =
        batchSystemMetadata.stream()
            .filter(
                sysMeta ->
                    Objects.nonNull(sysMeta)
                        && sysMeta.getProperties() != null
                        && sysMeta.getProperties().get(TELEMETRY_TRACE_KEY) != null
                        && sysMeta.getProperties().get(TELEMETRY_QUEUE_SPAN_KEY) != null)
            .collect(Collectors.toList());

    // resume log tracing
    logTracingEnabled.set(
        tracingEnabledSystemMetadata.stream()
            .anyMatch(
                sysMeta ->
                    Boolean.parseBoolean(
                        sysMeta.getProperties().getOrDefault(TELEMETRY_LOG_KEY, "false"))));

    // Create the span builder, close queue span and add links
    SpanBuilder spanBuilder =
        tracer.spanBuilder(name).setAttribute(BATCH_SIZE_ATTR, batchSystemMetadata.size());

    List<SpanContext> originalSpanContexts =
        tracingEnabledSystemMetadata.stream()
            .map(sysMeta -> closeQueueSpan(name, sysMeta, topicName))
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toList());

    Span span;
    if (originalSpanContexts.size() == 1) {
      // set parent if there is only a single original trace in the batch
      spanBuilder.setParent(Context.current().with(Span.wrap(originalSpanContexts.get(0))));
      span = spanBuilder.startSpan();
    } else {
      // otherwise link the current trace to all original traces
      originalSpanContexts.forEach(spanBuilder::addLink);
      span = spanBuilder.startSpan();

      // log linked traces
      if (isLogTracingEnabled()) {
        log.info(
            "Trace: {}, Linked Traces: {}",
            span.getSpanContext().getTraceId(),
            originalSpanContexts.stream().map(SpanContext::getTraceId));
      }
    }

    try (var scope = span.makeCurrent()) {
      // Set additional attributes
      for (int i = 0; i < attributes.length; i += 2) {
        span.setAttribute(attributes[i], attributes[i + 1]);
      }

      operation.run();
    } catch (Exception e) {
      span.setStatus(StatusCode.ERROR, e.getMessage());
      span.recordException(e);
      span.setAttribute(MetricUtils.ERROR_TYPE, e.getClass().getName());
      throw e;
    } finally {
      span.end();
    }
  }

  public SystemMetadata withTraceId(@Nonnull SystemMetadata systemMetadata, boolean force) {
    if (systemMetadata.getProperties() == null
        || force
        || !systemMetadata.getProperties().containsKey(TELEMETRY_TRACE_KEY)) {
      SpanContext currentSpanContext = Span.current().getSpanContext();

      if (currentSpanContext.isValid()) {
        SystemMetadata copy = GenericRecordUtils.copy(systemMetadata, SystemMetadata.class);

        if (!copy.hasProperties() || copy.getProperties() == null) {
          copy.setProperties(new StringMap());
        }

        copy.getProperties().putAll(Map.of(TELEMETRY_TRACE_KEY, currentSpanContext.getTraceId()));

        return copy;
      }
    }

    return systemMetadata;
  }

  /** Method to capture the current trace and span ids in systemMetadata */
  public SystemMetadata withProducerTrace(
      String operationName, @Nonnull SystemMetadata systemMetadata, String topicName) {
    SpanContext currentSpanContext = Span.current().getSpanContext();

    if (currentSpanContext.isValid()) {
      SystemMetadata copy = GenericRecordUtils.copy(systemMetadata, SystemMetadata.class);

      if (!copy.hasProperties() || copy.getProperties() == null) {
        copy.setProperties(new StringMap());
      }

      // Create the queue span that will be closed by consumer
      Span queueSpan =
          tracer
              .spanBuilder(operationName)
              .setParent(Context.current())
              .setSpanKind(SpanKind.PRODUCER)
              .setAttribute(MetricUtils.MESSAGING_SYSTEM, "kafka")
              .setAttribute(MetricUtils.MESSAGING_DESTINATION, topicName)
              .setAttribute(MetricUtils.MESSAGING_DESTINATION_KIND, "topic")
              .setAttribute(MetricUtils.MESSAGING_OPERATION, "publish")
              .startSpan();

      long enqueuedAt = Instant.now().toEpochMilli();
      if (!copy.getProperties().containsKey(TELEMETRY_TRACE_KEY)) {
        copy.getProperties()
            .putAll(
                Map.of(
                    TELEMETRY_TRACE_KEY, currentSpanContext.getTraceId(),
                    TELEMETRY_QUEUE_SPAN_KEY, queueSpan.getSpanContext().getSpanId()));
      }

      copy.getProperties()
          .putAll(
              Map.of(
                  TELEMETRY_LOG_KEY, String.valueOf(isLogTracingEnabled()),
                  TELEMETRY_ENQUEUED_AT, String.valueOf(enqueuedAt)));

      // It will be mirrored by consumer with enqueued time
      queueSpan.setAttribute(QUEUE_ENQUEUED_AT_ATTR, enqueuedAt).end();

      return copy;
    }

    return systemMetadata;
  }

  /**
   * When processing from queue - create new span with stored parent context
   *
   * @param systemMetadata systemMetadata with trace/span ids to restore
   */
  @Nullable
  private Span queueConsumerTrace(
      String operationName, @Nonnull SystemMetadata systemMetadata, String topicName) {

    SpanContext queueSpanContext = closeQueueSpan(operationName, systemMetadata, topicName);

    if (queueSpanContext != null) {
      // Create the processing span with the queue span as parent
      return tracer
          .spanBuilder(operationName)
          .setParent(
              Context.current()
                  .with(Span.wrap(queueSpanContext))) // Use queue span context as parent
          .startSpan();
    }

    return null;
  }

  @Nullable
  private SpanContext closeQueueSpan(
      String operationName, SystemMetadata metadata, String topicName) {
    if (metadata != null && metadata.getProperties() != null) {
      // resume log tracing
      logTracingEnabled.set(
          Boolean.parseBoolean(metadata.getProperties().getOrDefault(TELEMETRY_LOG_KEY, "false")));

      String traceId = metadata.getProperties().get(TELEMETRY_TRACE_KEY);
      String queueSpanId = metadata.getProperties().get(TELEMETRY_QUEUE_SPAN_KEY);

      if (traceId != null && queueSpanId != null) {

        SpanContext queueSpanContext =
            SpanContext.createFromRemoteParent(
                traceId, queueSpanId, TraceFlags.getSampled(), TraceState.getDefault());

        // Get the span and end it with duration
        SpanBuilder queueSpanBuilder =
            tracer
                .spanBuilder(operationName)
                .setParent(Context.current().with(Span.wrap(queueSpanContext)))
                .setSpanKind(SpanKind.CONSUMER);

        Span queueSpan =
            queueSpanBuilder
                .startSpan()
                .setAttribute(MetricUtils.MESSAGING_SYSTEM, "kafka")
                .setAttribute(MetricUtils.MESSAGING_DESTINATION, topicName)
                .setAttribute(MetricUtils.MESSAGING_DESTINATION_KIND, "topic")
                .setAttribute(MetricUtils.MESSAGING_OPERATION, "receive");

        // calculate duration
        if (metadata.getProperties().containsKey(TELEMETRY_ENQUEUED_AT)) {
          long enqueuedAt = Long.parseLong(metadata.getProperties().get(TELEMETRY_ENQUEUED_AT));
          long queueTimeMillis = Instant.now().toEpochMilli() - enqueuedAt;
          queueSpan
              .setAttribute(QUEUE_ENQUEUED_AT_ATTR, enqueuedAt)
              .setAttribute(QUEUE_DURATION_MS_ATTR, queueTimeMillis);
        }

        queueSpan.end();

        return queueSpanContext;
      }
    }

    return null;
  }

  private static class ConditionalLogSpanExporter implements SpanExporter {

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
      if (isLogTracingEnabled()) {
        spans.forEach(
            span -> {
              log.info(
                  "Trace: {}, SpanId: {}, ParentId: {}, Name: {}, Duration: {} ms",
                  span.getTraceId(),
                  span.getSpanId(),
                  span.getParentSpanId(),
                  span.getName(),
                  String.format(
                      "%.2f", (span.getEndEpochNanos() - span.getStartEpochNanos()) / 1_000_000.0));

              if (!span.getAttributes().isEmpty()) {
                log.info("Trace: {}, Attributes: {}", span.getTraceId(), span.getAttributes());
              }

              if (!span.getEvents().isEmpty()) {
                log.info("Trace: {}, Events: {}", span.getTraceId(), span.getEvents());
              }

              // Add logging for links
              if (!span.getLinks().isEmpty()) {
                span.getLinks()
                    .forEach(
                        link -> {
                          log.info(
                              "Trace: {}, Linked TraceId: {}, Linked SpanId: {}, Link Attributes: {}",
                              span.getTraceId(),
                              link.getSpanContext().getTraceId(),
                              link.getSpanContext().getSpanId(),
                              link.getAttributes());
                        });
              }
            });
      }

      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }
  }
}
