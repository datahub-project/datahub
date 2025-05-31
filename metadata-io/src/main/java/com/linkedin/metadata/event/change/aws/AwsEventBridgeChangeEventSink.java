package com.linkedin.metadata.event.change.aws;

import com.datahub.util.RecordUtils;
import com.linkedin.metadata.event.change.EntityChangeEventSink;
import com.linkedin.metadata.event.change.EntityChangeEventSinkConfig;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * An implementation of {@link EntityChangeEventSink} that sinks change events to AWS EventBridge.
 *
 * <p>This sink requires the following configs:
 *
 * <p>- region: The AWS region where the event bus is located. - eventBus: The ARN of the customer
 * event bus.
 *
 * <p>In addition, the following optional configs are allowed:
 *
 * <p>- accessKeyId: the aws access key id for the source account - secretAccessKey: the aws secret
 * access key for the source account
 */
@Slf4j
public class AwsEventBridgeChangeEventSink implements EntityChangeEventSink {

  public static final String AWS_ROLE_SESSION_NAME = "acryl-datahub-eventbridge-sink";
  private static final String AWS_REGION_PARAM = "region";
  private static final String AWS_EVENT_BUS_PARAM = "eventBus";
  private static final String AWS_ACCESS_KEY_ID_PARAM = "accessKeyId";
  private static final String AWS_SECRET_ACCESS_KEY_PARAM = "secretAccessKey";
  private static final String AWS_EVENT_BRIDGE_ASSUME_ROLE_ARN = "assumeRoleArn";
  private static final String AWS_EVENT_BRIDGE_EXTERNAL_ID = "externalId";
  private EventBridgeClient client;
  private String eventBus;

  @Override
  public void init(@Nonnull EntityChangeEventSinkConfig cfg) {
    try {
      this.eventBus =
          Objects.requireNonNull((String) cfg.getStaticConfig().get(AWS_EVENT_BUS_PARAM));
      final String region =
          Objects.requireNonNull((String) cfg.getStaticConfig().get(AWS_REGION_PARAM));
      final AwsCredentialsProvider provider = getAwsCredentialsProvider(cfg.getStaticConfig());
      this.client =
          EventBridgeClient.builder()
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
    PutEventsRequestEntry reqEntry =
        PutEventsRequestEntry.builder()
            .source("acryl.events")
            .detailType("EntityChangeEvent_v1")
            .detail(RecordUtils.toJsonString(changeEvent))
            .eventBusName(this.eventBus)
            .build();

    // TODO: Batch the events at the source. Then dump off after a certain time policy.
    final PutEventsRequest eventsRequest = PutEventsRequest.builder().entries(reqEntry).build();

    final PutEventsResponse result = this.client.putEvents(eventsRequest);

    for (PutEventsResultEntry resultEntry : result.entries()) {
      if (resultEntry.eventId() != null) {
        log.info("Successfully sinked event to EventBridge. Event id: {}", resultEntry.eventId());
      } else {
        throw new RuntimeException(
            String.format(
                "Failed to produce event to EventBridge! Error code: %s", resultEntry.errorCode()));
      }
    }
  }

  private AwsCredentialsProvider getAwsCredentialsProvider(final Map<String, Object> staticConfig) {
    AwsCredentialsProvider baseCredentials;

    if (staticConfig.containsKey(AWS_ACCESS_KEY_ID_PARAM)
        && staticConfig.containsKey(AWS_SECRET_ACCESS_KEY_PARAM)) {
      baseCredentials =
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                  (String) staticConfig.get(AWS_ACCESS_KEY_ID_PARAM),
                  (String) staticConfig.get(AWS_SECRET_ACCESS_KEY_PARAM)));
    } else {
      baseCredentials = DefaultCredentialsProvider.create();
    }

    final String roleArn = (String) staticConfig.get(AWS_EVENT_BRIDGE_ASSUME_ROLE_ARN);
    if (StringUtils.isBlank(roleArn)) {
      return baseCredentials;
    }

    /**
     * If a role to be assumed is configured, the "base" credentials are used to authenticate with
     * STS and perform the role assumption. StsAssumeRoleCredentialsProvider periodically sends
     * AssumeRoleRequest to STS to maintain/refresh the temporary session/credentials.
     */
    StsClientBuilder stsClientBuilder =
        StsClient.builder()
            .credentialsProvider(baseCredentials)
            .region(Region.of((String) staticConfig.get(AWS_REGION_PARAM)));

    AssumeRoleRequest.Builder assumeRoleBuilder =
        AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName(AWS_ROLE_SESSION_NAME);

    final String externalId = (String) staticConfig.get(AWS_EVENT_BRIDGE_EXTERNAL_ID);
    if (StringUtils.isNotBlank(externalId)) {
      assumeRoleBuilder.externalId(externalId);
    }

    return StsAssumeRoleCredentialsProvider.builder()
        .stsClient(stsClientBuilder.build())
        .refreshRequest(assumeRoleBuilder.build())
        .build();
  }
}
