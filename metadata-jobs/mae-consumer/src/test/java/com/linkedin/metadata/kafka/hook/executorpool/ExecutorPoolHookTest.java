package com.linkedin.metadata.kafka.hook.executorpool;

import static com.linkedin.metadata.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RequiredFieldNotPresentException;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.executorpool.*;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.regions.Region;
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

public class ExecutorPoolHookTest {

  @Mock private SqsClient mockSqsClient;

  private ExecutorPoolHook _executorPoolHookHook;
  private SystemEntityClient mockSystemEntityClient;
  private final Region mockRegion = Region.of("us-west2");
  private final String mockPoolId = "test-remote";
  private final String mockCustomerId = "123456-test01";
  private final String mockExecutorRoleArn =
      String.format("aws:iam::12345678:role/%s-remote-executor-12345", mockCustomerId);
  private final String mockPolicyTemplate =
      """
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Sid": "Test",
          "Effect": "Allow",
          "Principal": {
            "AWS": "{{roleArn}}"
          },
          "Action": [
              "sqs:SendMessage",
              "sqs:ReceiveMessage",
          ],
          "Resource": "{{queueArn}}"
        }
      ]
    }""";

  private MetadataChangeLog createMockEvent(RemoteExecutorPoolStatus status, ChangeType changeType)
      throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME);
    event.setAspectName(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);
    event.setChangeType(changeType);

    final RemoteExecutorPoolInfo poolInfo = new RemoteExecutorPoolInfo();
    final RemoteExecutorPoolState poolState = new RemoteExecutorPoolState();

    poolState.setStatus(RemoteExecutorPoolStatus.PROVISIONING_PENDING);
    poolInfo.setState(poolState);
    poolInfo.setIsEmbedded(false);

    event.setAspect(GenericRecordUtils.serializeAspect(poolInfo));
    event.setEntityUrn(
        Urn.createFromString(String.format("urn:li:dataHubRemoteExecutorPool:%s", mockPoolId)));
    return event;
  }

  @BeforeMethod
  public void setupTest() {
    MockitoAnnotations.openMocks(this);

    mockSystemEntityClient = Mockito.mock(SystemEntityClient.class);
    _executorPoolHookHook =
        new ExecutorPoolHook(
            mockSqsClient,
            mockRegion,
            mockSystemEntityClient,
            mockCustomerId,
            mockExecutorRoleArn,
            mockPolicyTemplate.replaceAll("\\s++", ""),
            "123",
            "true",
            "3600",
            true,
            "test",
            1,
            10);
    _executorPoolHookHook.init(TestOperationContexts.systemContextNoSearchAuthorization());
  }

  @Test
  public void testInvokeCreateQueueSuccess() throws Exception {

    String expectedQueueName = String.format("re-%s-744b196eec716095", mockCustomerId);
    String expectedQueueUrl = String.format("https://sqs.aws/%s", expectedQueueName);
    String expectedQueueArn =
        String.format("arn:aws:sqs:%s:1234567:%s", mockRegion, expectedQueueName);

    // createQueue should return queue URL
    CreateQueueRequest expectedQueueRequest =
        CreateQueueRequest.builder().queueName(expectedQueueName).build();
    CreateQueueResponse queueResponse =
        CreateQueueResponse.builder().queueUrl(expectedQueueUrl).build();
    Mockito.when(mockSqsClient.createQueue(expectedQueueRequest)).thenReturn(queueResponse);

    // getQueueAttributes should return queue ARN
    GetQueueAttributesRequest expectedGetAttributesRequest =
        GetQueueAttributesRequest.builder()
            .queueUrl(expectedQueueUrl)
            .attributeNames(List.of(QueueAttributeName.QUEUE_ARN))
            .build();

    Map<QueueAttributeName, String> getAttributes = new HashMap<>();
    getAttributes.put(QueueAttributeName.QUEUE_ARN, expectedQueueArn);
    GetQueueAttributesResponse getAttributesResponse =
        GetQueueAttributesResponse.builder().attributes(getAttributes).build();
    Mockito.when(mockSqsClient.getQueueAttributes(expectedGetAttributesRequest))
        .thenReturn(getAttributesResponse);

    // now verify that setQueueAttributes is called
    MetadataChangeLog event =
        createMockEvent(RemoteExecutorPoolStatus.PROVISIONING_PENDING, ChangeType.UPSERT);
    _executorPoolHookHook.invoke(event);

    String policyDocument =
        String.format(
                """
      {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Sid": "Test",
            "Effect": "Allow",
            "Principal": {
              "AWS": "%s"
            },
            "Action": [
              "sqs:SendMessage",
              "sqs:ReceiveMessage",
            ],
            "Resource": "%s"
          }
      ]
    }""",
                mockExecutorRoleArn, expectedQueueArn)
            .replaceAll("\\s++", "");

    Map<QueueAttributeName, String> setAttributes = new HashMap<>();
    setAttributes.put(QueueAttributeName.MESSAGE_RETENTION_PERIOD, "123");
    setAttributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, "3600");
    setAttributes.put(QueueAttributeName.SQS_MANAGED_SSE_ENABLED, "true");
    setAttributes.put(QueueAttributeName.POLICY, policyDocument);

    SetQueueAttributesRequest expectedSetAttributesRequest =
        SetQueueAttributesRequest.builder()
            .queueUrl(expectedQueueUrl)
            .attributes(setAttributes)
            .build();

    Mockito.verify(mockSqsClient, Mockito.times(1))
        .setQueueAttributes(Mockito.eq(expectedSetAttributesRequest));
  }

  @Test
  public void testInvokeCreateQueueFailure() throws Exception {

    MetadataChangeLog event =
        createMockEvent(RemoteExecutorPoolStatus.PROVISIONING_PENDING, ChangeType.UPSERT);
    _executorPoolHookHook.invoke(event);

    Mockito.verify(mockSqsClient, Mockito.never())
        .getQueueAttributes(Mockito.any(GetQueueAttributesRequest.class));
  }

  @Test
  public void testInvokeDeleteQueueSuccess() throws Exception {

    MetadataChangeLog event = createMockEvent(RemoteExecutorPoolStatus.READY, ChangeType.DELETE);

    String expectedQueueName = String.format("re-%s-744b196eec716095", mockCustomerId);
    String expectedQueueUrl = String.format("https://sqs.aws/%s", expectedQueueName);

    GetQueueUrlRequest expectedUrlRequest =
        GetQueueUrlRequest.builder().queueName(expectedQueueName).build();
    GetQueueUrlResponse expectedUrlResponse =
        GetQueueUrlResponse.builder().queueUrl(expectedQueueUrl).build();
    Mockito.when(mockSqsClient.getQueueUrl(expectedUrlRequest)).thenReturn(expectedUrlResponse);

    _executorPoolHookHook.invoke(event);

    DeleteQueueRequest expectedQueueRequest =
        DeleteQueueRequest.builder().queueUrl(expectedQueueUrl).build();
    Mockito.verify(mockSqsClient, Mockito.times(1)).deleteQueue(Mockito.eq(expectedQueueRequest));
  }

  @Test
  public void testInvokeDeleteQueueFailure() throws Exception {
    MetadataChangeLog event = createMockEvent(RemoteExecutorPoolStatus.READY, ChangeType.DELETE);

    String expectedQueueName = String.format("re-%s-744b196eec716095", mockCustomerId);
    String expectedQueueUrl = String.format("https://sqs.aws/%s", expectedQueueName);

    GetQueueUrlRequest expectedUrlRequest =
        GetQueueUrlRequest.builder().queueName(expectedQueueName).build();
    Mockito.when(mockSqsClient.getQueueUrl(expectedUrlRequest))
        .thenThrow(new RuntimeException("AWS API Exception"));

    _executorPoolHookHook.invoke(event);

    DeleteQueueRequest expectedQueueRequest =
        DeleteQueueRequest.builder().queueUrl(expectedQueueUrl).build();
    Mockito.verify(mockSqsClient, Mockito.never()).deleteQueue(expectedQueueRequest);
  }

  @Test
  public void testGetPoolInfoFromEvent() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setAspectName("non-existent-aspect");
    event.setChangeType(ChangeType.UPSERT);
    assertThrows(RequiredFieldNotPresentException.class, () -> _executorPoolHookHook.invoke(event));
  }
}
