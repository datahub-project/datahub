package com.linkedin.metadata.dataHubUsage.aws;

import static com.linkedin.metadata.event.change.aws.AwsEventBridgeChangeEventSink.AWS_ROLE_SESSION_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.config.aws.EventBridgeConfiguration;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class EventBridgeBatchProcessor implements Closeable {
  private final EventBridgeClient eventBridgeClient;
  private final List<PutEventsRequestEntry> eventBuffer = new ArrayList<>();
  private final int maxBatchSize;
  private final int maxRetries;
  private final ScheduledExecutorService scheduler;
  private final String eventBus;

  public EventBridgeBatchProcessor(EventBridgeConfiguration eventBridgeConfiguration) {
    final String region = eventBridgeConfiguration.getRegion();

    if (StringUtils.isNotBlank(region)) {
      final AwsCredentialsProvider provider = getAwsCredentialsProvider(eventBridgeConfiguration);
      this.eventBridgeClient =
          EventBridgeClient.builder()
              .region(Region.of(region))
              .credentialsProvider(provider)
              .httpClientBuilder(ApacheHttpClient.builder())
              .build();
    } else {
      // Try and create default client, may error. Should not enable hook without specifying region
      eventBridgeClient =
          EventBridgeClient.builder()
              .credentialsProvider(DefaultCredentialsProvider.create())
              .region(DefaultAwsRegionProviderChain.builder().build().getRegion())
              .build();
    }
    this.maxBatchSize =
        Math.min(eventBridgeConfiguration.getMaxBatchSize(), 10); // EventBridge limit is 10
    this.maxRetries = eventBridgeConfiguration.getMaxRetries();
    this.scheduler = Executors.newScheduledThreadPool(1);
    this.eventBus = eventBridgeConfiguration.getEventBus();

    // Schedule periodic flush
    this.scheduler.scheduleAtFixedRate(
        this::flush,
        eventBridgeConfiguration.getFlushIntervalSeconds(),
        eventBridgeConfiguration.getFlushIntervalSeconds(),
        TimeUnit.SECONDS);
  }

  public synchronized void addEvent(JsonNode event, String eventType) {
    PutEventsRequestEntry reqEntry =
        PutEventsRequestEntry.builder()
            .source("acryl.events")
            .detailType(eventType)
            .detail(event.toString())
            .eventBusName(eventBus)
            .build();
    eventBuffer.add(reqEntry);
    if (eventBuffer.size() >= maxBatchSize) {
      flush();
    }
  }

  public synchronized void flush() {
    if (eventBuffer.isEmpty()) {
      return;
    }

    List<PutEventsRequestEntry> batch = new ArrayList<>(eventBuffer);
    eventBuffer.clear();

    sendBatchWithRetry(batch, 0);
  }

  private void sendBatchWithRetry(List<PutEventsRequestEntry> batch, int retryCount) {
    try {
      PutEventsRequest request = PutEventsRequest.builder().entries(batch).build();

      PutEventsResponse result = eventBridgeClient.putEvents(request);

      // Check for failed entries and retry if needed
      if (result.failedEntryCount() > 0) {
        List<PutEventsRequestEntry> failedEntries = new ArrayList<>();

        for (int i = 0; i < batch.size(); i++) {
          PutEventsResultEntry resultEntry = result.entries().get(i);
          if (resultEntry.errorCode() != null) {
            failedEntries.add(batch.get(i));
          }
        }

        if (!failedEntries.isEmpty() && retryCount < maxRetries) {
          // Add exponential backoff before retry
          long backoffMillis = (long) Math.pow(2, retryCount) * 100;
          scheduler.schedule(
              () -> sendBatchWithRetry(failedEntries, retryCount + 1),
              backoffMillis,
              TimeUnit.MILLISECONDS);
        }
      }
    } catch (Exception e) {
      // Handle exception - could retry the whole batch after backoff
      if (retryCount < maxRetries) {
        long backoffMillis = (long) Math.pow(2, retryCount) * 100;
        scheduler.schedule(
            () -> sendBatchWithRetry(batch, retryCount + 1), backoffMillis, TimeUnit.MILLISECONDS);
      }
    }
  }

  private AwsCredentialsProvider getAwsCredentialsProvider(
      EventBridgeConfiguration eventBridgeConfiguration) {
    AwsCredentialsProvider baseCredentials = DefaultCredentialsProvider.create();

    final String roleArn = eventBridgeConfiguration.getAssumeRoleArn();
    if (StringUtils.isBlank(roleArn)) {
      return baseCredentials;
    }

    /*
     * If a role to be assumed is configured, the "base" credentials are used to authenticate with
     * STS and perform the role assumption. StsAssumeRoleCredentialsProvider periodically sends
     * AssumeRoleRequest to STS to maintain/refresh the temporary session/credentials.
     */
    StsClientBuilder stsClientBuilder =
        StsClient.builder()
            .credentialsProvider(baseCredentials)
            .region(Region.of(eventBridgeConfiguration.getRegion()));

    AssumeRoleRequest.Builder assumeRoleBuilder =
        AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName(AWS_ROLE_SESSION_NAME);

    final String externalId = eventBridgeConfiguration.getExternalId();
    if (StringUtils.isNotBlank(externalId)) {
      assumeRoleBuilder.externalId(externalId);
    }

    return StsAssumeRoleCredentialsProvider.builder()
        .stsClient(stsClientBuilder.build())
        .refreshRequest(assumeRoleBuilder.build())
        .build();
  }

  @Override
  public void close() {
    flush();
    scheduler.shutdown();
  }
}
