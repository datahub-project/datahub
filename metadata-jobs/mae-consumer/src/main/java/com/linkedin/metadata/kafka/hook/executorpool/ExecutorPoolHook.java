package com.linkedin.metadata.kafka.hook.executorpool;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.executorpool.*;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

/** This hook provisions an SQS queue for each newly created Executor Pool aspect */
@Slf4j
@Component
public class ExecutorPoolHook implements MetadataChangeLogHook {
  private final boolean isEnabled;
  private final String customerId;
  private final String executorRoleArn;
  private final String policyTemplate;
  private final String messageRetentionPeriod;
  private final String managedSseEnabled;
  private final String visibilityTimeout;
  @Getter private final String consumerGroupSuffix;
  private final int sqsMaxRetries;
  private final int sqsBaseDelayMs;

  private SqsClient sqsClient;
  private Region region;
  private final SystemEntityClient systemEntityClient;
  private OperationContext systemOperationContext;

  @Autowired
  public ExecutorPoolHook(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull @Value("${executors.executorCustomerId}") String customerId,
      @Nonnull @Value("${executors.executorRoleArn}") String executorRoleArn,
      @Nonnull @Value("${executors.executorPoolHook.policyTemplate}") String policyTemplate,
      @Nonnull @Value("${executors.executorPoolHook.messageRetentionPeriod}")
          String messageRetentionPeriod,
      @Nonnull @Value("${executors.executorPoolHook.managedSseEnabled}") String managedSseEnabled,
      @Nonnull @Value("${executors.executorPoolHook.visibilityTimeout}") String visibilityTimeout,
      @Nonnull @Value("${executors.executorPoolHook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${executors.executorPoolHook.consumerGroupSuffix}")
          String consumerGroupSuffix,
      @Nonnull @Value("${executors.executorPoolHook.sqsMaxRetries:5}") int sqsMaxRetries,
      @Nonnull @Value("${executors.executorPoolHook.sqsBaseDelayMs:500}") int sqsBaseDelayMs) {
    this.systemEntityClient = systemEntityClient;
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
    this.customerId = customerId;
    this.executorRoleArn = executorRoleArn;
    this.policyTemplate = policyTemplate;
    this.messageRetentionPeriod = messageRetentionPeriod;
    this.managedSseEnabled = managedSseEnabled;
    this.visibilityTimeout = visibilityTimeout;
    this.sqsMaxRetries = sqsMaxRetries;
    this.sqsBaseDelayMs = sqsBaseDelayMs;

    try {
      this.region = DefaultAwsRegionProviderChain.builder().build().getRegion();
      this.sqsClient = SqsClient.builder().region(region).build();
    } catch (Exception e) {
      this.sqsClient = null;
      log.error("Unable to create SqsClient instance. ExecutorPoolHook is disabled.");
    }

    log.info(
        String.format(
            "Initialized ExecutorPoolHook: customerId:%s; executorRoleArn:%s; messageRetentionPeriod:%s; managedSseEnabled:%s; visibilityTimeout:%s",
            customerId,
            executorRoleArn,
            messageRetentionPeriod,
            managedSseEnabled,
            visibilityTimeout));
  }

  public ExecutorPoolHook(
      @Nonnull final SqsClient sqsClient,
      @Nonnull final Region region,
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull String customerId,
      @Nonnull String executorRoleArn,
      @Nonnull String policyTemplate,
      @Nonnull String messageRetentionPeriod,
      @Nonnull String managedSseEnabled,
      @Nonnull String visibilityTimeout,
      @Nonnull Boolean isEnabled,
      @Nonnull String consumerGroupSuffix,
      @Nonnull int sqsMaxRetries,
      @Nonnull int sqsBaseDelayMs) {
    this.sqsClient = sqsClient;
    this.region = region;
    this.systemEntityClient = systemEntityClient;
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
    this.customerId = customerId;
    this.executorRoleArn = executorRoleArn;
    this.policyTemplate = policyTemplate;
    this.messageRetentionPeriod = messageRetentionPeriod;
    this.managedSseEnabled = managedSseEnabled;
    this.visibilityTimeout = visibilityTimeout;
    this.sqsMaxRetries = sqsMaxRetries;
    this.sqsBaseDelayMs = sqsBaseDelayMs;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public ExecutorPoolHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    return this;
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    if (this.sqsClient == null) {
      return;
    }

    if (isExecutorPoolCreated(event)) {
      log.info(
          "Received {} to executor pool {}. Creating SQS queue.",
          event.getChangeType(),
          event.getEntityUrn());
      provisionSqsQueue(event);
    } else if (isExecutorPoolDeleted(event)) {
      log.info(
          "Received {} to executor pool {}. Deleting SQS queue.",
          event.getChangeType(),
          event.getEntityUrn());
      deleteSqsQueue(event);
    }
  }

  private void updateExecutorPoolState(
      MetadataChangeLog event,
      RemoteExecutorPoolStatus newStatus,
      String errorMessage,
      String queueUrl) {

    RemoteExecutorPoolInfo poolInfo = getPoolInfoFromEvent(event);
    RemoteExecutorPoolState poolState = poolInfo.getState();

    poolState.setStatus(newStatus);
    poolState.setMessage(errorMessage);
    poolInfo.setQueueUrl(queueUrl);
    poolInfo.setIsEmbedded(false);
    poolInfo.setQueueRegion(region.toString());

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    GenericAspect serializedAspect = GenericRecordUtils.serializeAspect(poolInfo);

    mcp.setAspect(serializedAspect);
    mcp.setAspectName(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);
    mcp.setEntityType(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setEntityUrn(event.getEntityUrn());

    try {
      systemEntityClient.ingestProposal(systemOperationContext, mcp, false);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to emit executor pool status update for entity %s", event.getEntityUrn()),
          e);
    }
  }

  @FunctionalInterface
  public interface RetryableOperation<T> {
    T execute() throws Exception;
  }

  public <T> T executeWithRetry(RetryableOperation<T> operation) throws Exception {
    int attempts = 0;
    while (attempts < sqsMaxRetries) {
      try {
        return operation.execute();
      } catch (Exception e) {
        log.error(
            String.format(
                "Error executing a SQS operation (attempt %d of %d): %s",
                attempts, sqsMaxRetries, e.getMessage()));
        attempts++;
        if (attempts >= sqsMaxRetries) {
          throw e;
        }
        long delay = (long) (sqsBaseDelayMs * Math.pow(2, attempts - 1));
        try {
          Thread.sleep(delay);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new Exception("Retry interrupted", ex);
        }
      }
    }
    throw new Exception("Maximum retry attempts exceeded");
  }

  private String renderPolicyTemplate(String queueArn) {
    String policyDocument =
        policyTemplate
            .replaceAll("\\{\\{queueArn\\}\\}", queueArn)
            .replaceAll("\\{\\{roleArn\\}\\}", executorRoleArn);
    return policyDocument;
  }

  private String getQueueName(String poolName) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] digest = md.digest(poolName.getBytes(StandardCharsets.UTF_8));
    String queueSuffix = HexFormat.of().formatHex(digest).substring(0, 16);
    return String.format("re-%s-%s", customerId, queueSuffix);
  }

  private void updateQueueAttributes(String queueUrl) throws Exception {
    GetQueueAttributesRequest getRequest =
        GetQueueAttributesRequest.builder()
            .queueUrl(queueUrl)
            .attributeNames(List.of(QueueAttributeName.QUEUE_ARN))
            .build();

    GetQueueAttributesResponse getResponse =
        executeWithRetry(
            () -> {
              return sqsClient.getQueueAttributes(getRequest);
            });
    Map<QueueAttributeName, String> getAttributes = getResponse.attributes();
    String queueArn = getAttributes.get(QueueAttributeName.QUEUE_ARN);

    String policyDocument = renderPolicyTemplate(queueArn);

    Map<QueueAttributeName, String> setAttributes = new HashMap<>();
    setAttributes.put(QueueAttributeName.MESSAGE_RETENTION_PERIOD, messageRetentionPeriod);
    setAttributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, visibilityTimeout);
    setAttributes.put(QueueAttributeName.SQS_MANAGED_SSE_ENABLED, managedSseEnabled);
    setAttributes.put(QueueAttributeName.POLICY, policyDocument);

    SetQueueAttributesRequest setRequest =
        SetQueueAttributesRequest.builder().queueUrl(queueUrl).attributes(setAttributes).build();
    executeWithRetry(
        () -> {
          return sqsClient.setQueueAttributes(setRequest);
        });
  }

  private void deleteSqsQueue(final MetadataChangeLog event) {

    try {
      final String queueName = getQueueName(event.getEntityUrn().getEntityKey().get(0));
      log.info(
          String.format(
              "Going to delete SQS queue %s for pool %s",
              queueName, event.getEntityUrn().getEntityKey().get(0)));

      // Queue URL is not available in DELETE event
      GetQueueUrlRequest getUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
      GetQueueUrlResponse getUrlResponse =
          executeWithRetry(
              () -> {
                return sqsClient.getQueueUrl(getUrlRequest);
              });
      String queueUrl = getUrlResponse.queueUrl();
      DeleteQueueRequest deleteQueueRequest =
          DeleteQueueRequest.builder().queueUrl(queueUrl).build();
      executeWithRetry(
          () -> {
            return sqsClient.deleteQueue(deleteQueueRequest);
          });

      log.info(
          "Successfully deleted SQS queue {} for Executor Pool {} in instance {}",
          queueName,
          event.getEntityUrn().getEntityKey().get(0),
          customerId);
    } catch (Exception e) {
      log.error(
          "Error deleting SQS queue for Executor Pool {} in instance {}: {}",
          event.getEntityUrn().getEntityKey().get(0),
          customerId,
          e.getMessage());
    }
  }

  private String createSqsQueue(final MetadataChangeLog event) throws Exception {
    final String queueName = getQueueName(event.getEntityUrn().getEntityKey().get(0));

    log.info(
        String.format(
            "Going to create SQS queue %s for pool %s",
            queueName, event.getEntityUrn().getEntityKey().get(0)));

    CreateQueueRequest createQueueRequest =
        CreateQueueRequest.builder().queueName(queueName).build();
    CreateQueueResponse createQueueResponse =
        executeWithRetry(
            () -> {
              return sqsClient.createQueue(createQueueRequest);
            });
    String queueUrl = createQueueResponse.queueUrl();

    return queueUrl;
  }

  private void provisionSqsQueue(final MetadataChangeLog event) {
    updateExecutorPoolState(event, RemoteExecutorPoolStatus.PROVISIONING_IN_PROGRESS, "", "");
    try {
      String queueUrl = createSqsQueue(event);
      updateQueueAttributes(queueUrl);
      updateExecutorPoolState(event, RemoteExecutorPoolStatus.READY, "", queueUrl);
      log.info(
          "Successfully created SQS queue {} for Executor Pool {} in instance {}",
          queueUrl,
          event.getEntityUrn().getEntityKey().get(0),
          customerId);
    } catch (Exception e) {
      log.error(
          "Error creating SQS queue for Executor Pool {} in instance {}: {}",
          event.getEntityUrn().getEntityKey().get(0),
          customerId,
          e.getMessage());
      updateExecutorPoolState(
          event,
          RemoteExecutorPoolStatus.PROVISIONING_FAILED,
          String.format("Error creating queue: %s", e.getMessage()),
          "");
    }
  }

  private boolean isExecutorPoolCreated(final MetadataChangeLog event) {
    return AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME.equals(event.getAspectName())
        && (ChangeType.UPSERT.equals(event.getChangeType())
            || ChangeType.CREATE.equals(event.getChangeType()))
        && getPoolInfoFromEvent(event).getState().getStatus()
            == RemoteExecutorPoolStatus.PROVISIONING_PENDING;
  }

  private boolean isExecutorPoolDeleted(final MetadataChangeLog event) {
    return AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME.equals(event.getEntityType())
        && ChangeType.DELETE.equals(event.getChangeType());
  }

  /**
   * Deserializes and returns an instance of {@link ExecutorPoolInfo} extracted from a {@link
   * MetadataChangeLog} event.
   */
  private RemoteExecutorPoolInfo getPoolInfoFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    try {
      entitySpec = systemOperationContext.getEntityRegistry().getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException(
          "Failed to get RemoteExecutorPoolInfo info from MetadataChangeLog event. Skipping processing.",
          e);
    }
    return (RemoteExecutorPoolInfo)
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            entitySpec.getAspectSpec(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME));
  }
}
