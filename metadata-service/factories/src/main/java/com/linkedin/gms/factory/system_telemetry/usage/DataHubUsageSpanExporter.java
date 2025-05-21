package com.linkedin.gms.factory.system_telemetry.usage;

import static com.linkedin.metadata.datahubusage.DataHubUsageEventConstants.*;
import static com.linkedin.metadata.datahubusage.DataHubUsageEventConstants.EVENT_SOURCE;
import static com.linkedin.metadata.datahubusage.DataHubUsageEventConstants.SOURCE_IP;
import static com.linkedin.metadata.datahubusage.DataHubUsageEventType.DELETE_ENTITY_EVENT;
import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.*;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.UsageExportConfiguration;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class DataHubUsageSpanExporter implements SpanExporter {

  private final Producer<String, String> producer;
  private final String topic;
  private final Set<String> eventTypes;
  private final Set<String> aspectTypes;
  private final Set<String> userFilters;

  public DataHubUsageSpanExporter(
      Producer<String, String> producer, String topic, UsageExportConfiguration config) {
    this.producer = producer;
    this.topic = topic;
    if (StringUtils.isNotBlank(config.getUsageEventTypes())) {
      this.eventTypes = Set.of(config.getUsageEventTypes().split(","));
    } else {
      this.eventTypes = Collections.emptySet();
    }

    if (StringUtils.isNotBlank(config.getAspectTypes())) {
      this.aspectTypes = Set.of(config.getAspectTypes().split(","));
    } else {
      this.aspectTypes = Collections.emptySet();
    }

    if (StringUtils.isNotBlank(config.getUserFilters())) {
      this.userFilters = Set.of(config.getUserFilters().split(","));
    } else {
      this.userFilters = Collections.emptySet();
    }
  }

  private static final AttributeKey<String> USER_ID_KEY = AttributeKey.stringKey(USER_ID_ATTR);

  private static final AttributeKey<String> EVENT_TYPE_KEY =
      AttributeKey.stringKey(EVENT_TYPE_ATTR);

  private static final AttributeKey<String> ENTITY_URN_KEY =
      AttributeKey.stringKey(ENTITY_URN_ATTR);

  private static final AttributeKey<String> ENTITY_TYPE_KEY =
      AttributeKey.stringKey(ENTITY_TYPE_ATTR);

  private static final AttributeKey<String> ASPECT_NAME_KEY =
      AttributeKey.stringKey(ASPECT_NAME_ATTR);

  private static final AttributeKey<String> EVENT_SOURCE_KEY =
      AttributeKey.stringKey(OpenTelemetryKeyConstants.EVENT_SOURCE);

  private static final AttributeKey<String> USER_AGENT_KEY =
      AttributeKey.stringKey(USER_AGENT_ATTR);

  private static final AttributeKey<String> LOGIN_SOURCE_KEY =
      AttributeKey.stringKey(LOGIN_SOURCE_ATTR);

  private static final AttributeKey<String> TELEMETRY_TRACE_ID_KEY =
      AttributeKey.stringKey(TELEMETRY_TRACE_ID_ATTR);

  private static final AttributeKey<String> SOURCE_IP_KEY =
      AttributeKey.stringKey(OpenTelemetryKeyConstants.SOURCE_IP);

  @Override
  public CompletableResultCode export(Collection<SpanData> spans) {
    spans.stream()
        .flatMap(span -> span.getEvents().stream())
        .filter(this::eventMatches)
        .forEach(this::recordEvent);

    return CompletableResultCode.ofSuccess();
  }

  private boolean eventMatches(EventData eventData) {
    return LOGIN_EVENT.equals(eventData.getName())
        || (UPDATE_ASPECT_EVENT.equals(eventData.getName()) && isConfiguredEvent(eventData));
  }

  private boolean isConfiguredEvent(EventData eventData) {
    String eventType = eventData.getAttributes().get(EVENT_TYPE_KEY);
    return (this.eventTypes.contains(eventType)
            || ((DataHubUsageEventType.UPDATE_ASPECT_EVENT.getType().equals(eventType)
                    || DELETE_ENTITY_EVENT.getType().equals(eventType))
                && this.aspectTypes.contains(eventData.getAttributes().get(ASPECT_NAME_KEY))))
        && userFilters.stream()
            .noneMatch(user -> user.equals(eventData.getAttributes().get(USER_ID_KEY)));
  }

  private void recordEvent(EventData event) {
    // Publish usage event to Usage Kafka Topic

    String actor = event.getAttributes().get(USER_ID_KEY);
    ObjectNode usageEvent = JsonNodeFactory.instance.objectNode();
    usageEvent.put(ACTOR_URN, actor);
    String type = event.getAttributes().get(EVENT_TYPE_KEY);
    usageEvent.put(TYPE, type);
    long timestamp = TimeUnit.of(ChronoUnit.NANOS).toMillis(event.getEpochNanos());
    usageEvent.put(TIMESTAMP, timestamp);
    String entityUrn = event.getAttributes().get(ENTITY_URN_KEY);
    if (StringUtils.isNotBlank(entityUrn)) {
      usageEvent.put(ENTITY_URN, entityUrn);
    }
    String entityType = event.getAttributes().get(ENTITY_TYPE_KEY);
    if (StringUtils.isNotBlank(entityType)) {
      usageEvent.put(ENTITY_TYPE, entityType);
    }
    String eventSource = event.getAttributes().get(EVENT_SOURCE_KEY);
    if (StringUtils.isNotBlank(eventSource)) {
      usageEvent.put(EVENT_SOURCE, eventSource);
    }
    String userAgent = event.getAttributes().get(USER_AGENT_KEY);
    if (StringUtils.isNotBlank(userAgent)) {
      usageEvent.put(USER_AGENT, userAgent);
    }
    String loginSource = event.getAttributes().get(LOGIN_SOURCE_KEY);
    if (StringUtils.isNotBlank(loginSource)) {
      usageEvent.put(LOGIN_SOURCE, loginSource);
    }
    String telemetryTraceId = event.getAttributes().get(TELEMETRY_TRACE_ID_KEY);
    if (StringUtils.isNotBlank(telemetryTraceId)) {
      usageEvent.put(TRACE_ID, telemetryTraceId);
    }
    String aspectName = event.getAttributes().get(ASPECT_NAME_KEY);
    if (StringUtils.isNotBlank(aspectName)) {
      usageEvent.put(ASPECT_NAME, aspectName);
    }
    String sourceIP = event.getAttributes().get(SOURCE_IP_KEY);
    if (StringUtils.isNotBlank(sourceIP)) {
      usageEvent.put(SOURCE_IP, sourceIP);
    }

    usageEvent.put(USAGE_SOURCE, BACKEND_SOURCE);
    log.debug(
        String.format("Emitting product analytics event. actor: %s, event: %s", actor, usageEvent));
    final ProducerRecord<String, String> record =
        new ProducerRecord<>(topic, actor, usageEvent.toString());
    producer.send(record);
  }

  @Override
  public CompletableResultCode flush() {
    producer.flush();
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode shutdown() {
    producer.flush();
    return CompletableResultCode.ofSuccess();
  }
}
