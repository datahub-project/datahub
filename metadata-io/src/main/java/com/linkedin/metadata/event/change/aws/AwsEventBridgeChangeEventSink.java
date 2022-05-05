package com.linkedin.metadata.event.change.aws;

import com.datahub.util.RecordUtils;
import com.linkedin.metadata.event.change.EntityChangeEventSink;
import com.linkedin.metadata.event.change.EntityChangeEventSinkConfig;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;


/**
 * An implementation of {@link EntityChangeEventSink} that sinks change events to
 * AWS event bridge.
 *
 * This sink requires the following configs:
 *
 *    - region: The AWS region where the event bus is located.
 *    - eventBus: The ARN of the customer event bus.
 *
 * In addition, the following optional configs are allowed:
 *
 *    - accessKeyId: the aws access key id for the source account
 *    - secretAccessKey: the aws secret access key for the source account
 */
@Slf4j
public class AwsEventBridgeChangeEventSink implements EntityChangeEventSink {

  private static final String AWS_REGION_PARAM = "region";
  private static final String AWS_EVENT_BUS_PARAM = "eventBus";
  private static final String AWS_ACCESS_KEY_ID_PARAM = "accessKeyId";
  private static final String AWS_SECRET_ACCESS_KEY_PARAM = "secretAccessKey";
  private EventBridgeClient client;
  private String eventBus;

  @Override
  public void init(@Nonnull EntityChangeEventSinkConfig cfg) {
    try {
      this.eventBus = Objects.requireNonNull((String) cfg.getStaticConfig().get(AWS_EVENT_BUS_PARAM));
      final String region = Objects.requireNonNull((String) cfg.getStaticConfig().get(AWS_REGION_PARAM));
      final AwsCredentialsProvider provider = getAwsProvider(cfg.getStaticConfig());
      this.client = EventBridgeClient.builder()
          .region(Region.of(region))
          .credentialsProvider(provider)
          .httpClientBuilder(ApacheHttpClient.builder())
          .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to init the change event hook", e);
    }
  }

  @Override
  public void sink(@Nonnull EntityChangeEvent changeEvent) {
    sinkToEventBridge(changeEvent);
  }

  private void sinkToEventBridge(@Nonnull EntityChangeEvent changeEvent) {
    PutEventsRequestEntry reqEntry = PutEventsRequestEntry.builder()
        .source("acryl.events")
        .detailType("EntityChangeEvent_v1")
        .detail(RecordUtils.toJsonString(changeEvent))
        .eventBusName(this.eventBus)
        .build();

    // TODO: Batch the events at the source. Then dump off after a certain time policy.
    final PutEventsRequest eventsRequest = PutEventsRequest.builder()
        .entries(reqEntry)
        .build();

    final PutEventsResponse result = this.client.putEvents(eventsRequest);

    for (PutEventsResultEntry resultEntry : result.entries()) {
      if (resultEntry.eventId() != null) {
        log.info("Successfully sinked event to event bridge. Event id: {}", resultEntry.eventId());
      } else {
        throw new RuntimeException(String.format("Failed to produce event to AWS event bridge! error code: %s", resultEntry.errorCode()));
      }
    }
  }

  private AwsCredentialsProvider getAwsProvider(final Map<String, Object> staticConfig) {
    if (staticConfig.containsKey(AWS_ACCESS_KEY_ID_PARAM) && staticConfig.containsKey(AWS_SECRET_ACCESS_KEY_PARAM)) {
      return StaticCredentialsProvider.create(AwsBasicCredentials.create(
          (String) staticConfig.get(AWS_ACCESS_KEY_ID_PARAM),
          (String) staticConfig.get(AWS_SECRET_ACCESS_KEY_PARAM)
      ));
    }
    return DefaultCredentialsProvider.create();
  }
}
