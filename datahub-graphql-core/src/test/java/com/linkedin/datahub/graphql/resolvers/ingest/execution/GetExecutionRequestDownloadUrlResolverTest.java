package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.GetExecutionRequestDownloadUrlInput;
import com.linkedin.datahub.graphql.generated.GetExecutionRequestDownloadUrlResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestArtifactsLocation;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.aws.S3Util;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@Test
public class GetExecutionRequestDownloadUrlResolverTest {

  private static final Urn TEST_EXECUTION_REQUEST_URN =
      Urn.createFromTuple(Constants.EXECUTION_REQUEST_ENTITY_NAME, "test-id");
  private static final String TEST_S3_LOCATION = "s3://test-bucket/path/to/file.tar.gz";
  private static final String TEST_PRESIGNED_URL =
      "https://s3.amazonaws.com/test-bucket/path/to/file.tar.gz?X-Amz-Signature=test";
  private static final String EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME =
      "dataHubExecutionRequestArtifactsLocation";

  @Test
  public void testGetDownloadUrlSuccess() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    S3Util mockS3Util = mock(S3Util.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = TestUtils.getMockAllowContext();

    GetExecutionRequestDownloadUrlInput input = new GetExecutionRequestDownloadUrlInput();
    input.setExecutionRequestUrn(TEST_EXECUTION_REQUEST_URN.toString());
    input.setExpirationSeconds(3600);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Mock ExecutionRequestArtifactsLocation aspect
    ExecutionRequestArtifactsLocation uploadLocation = new ExecutionRequestArtifactsLocation();
    uploadLocation.setLocation(TEST_S3_LOCATION);

    EnvelopedAspect envelopedAspect = mock(EnvelopedAspect.class);
    Aspect mockAspect = mock(Aspect.class);
    when(envelopedAspect.getValue()).thenReturn(mockAspect);
    when(mockAspect.data()).thenReturn(uploadLocation.data());

    EntityResponse entityResponse = mock(EntityResponse.class);
    EnvelopedAspectMap aspectMap = mock(EnvelopedAspectMap.class);
    when(aspectMap.containsKey(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME)).thenReturn(true);
    when(aspectMap.get(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME))
        .thenReturn(envelopedAspect);
    when(entityResponse.getAspects()).thenReturn(aspectMap);

    when(mockClient.batchGetV2(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_EXECUTION_REQUEST_URN)),
            any()))
        .thenReturn(ImmutableMap.of(TEST_EXECUTION_REQUEST_URN, entityResponse));

    when(mockS3Util.generatePresignedDownloadUrl("test-bucket", "path/to/file.tar.gz", 3600))
        .thenReturn(TEST_PRESIGNED_URL);

    GetExecutionRequestDownloadUrlResolver resolver =
        new GetExecutionRequestDownloadUrlResolver(mockClient, mockS3Util);

    // Execute
    GetExecutionRequestDownloadUrlResult result = resolver.get(mockEnv).join();

    // Verify
    assertEquals(result.getDownloadUrl(), TEST_PRESIGNED_URL);
    assertEquals((int) result.getExpiresIn(), 3600);
    verify(mockS3Util).generatePresignedDownloadUrl("test-bucket", "path/to/file.tar.gz", 3600);
  }

  @Test
  public void testGetDownloadUrlWithDefaultExpiration() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    S3Util mockS3Util = mock(S3Util.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = TestUtils.getMockAllowContext();

    GetExecutionRequestDownloadUrlInput input = new GetExecutionRequestDownloadUrlInput();
    input.setExecutionRequestUrn(TEST_EXECUTION_REQUEST_URN.toString());
    // No expiration seconds set, should default to 3600

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Mock ExecutionRequestArtifactsLocation aspect
    ExecutionRequestArtifactsLocation uploadLocation = new ExecutionRequestArtifactsLocation();
    uploadLocation.setLocation(TEST_S3_LOCATION);

    EnvelopedAspect envelopedAspect = mock(EnvelopedAspect.class);
    Aspect mockAspect = mock(Aspect.class);
    when(envelopedAspect.getValue()).thenReturn(mockAspect);
    when(mockAspect.data()).thenReturn(uploadLocation.data());

    EntityResponse entityResponse = mock(EntityResponse.class);
    EnvelopedAspectMap aspectMap = mock(EnvelopedAspectMap.class);
    when(aspectMap.containsKey(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME)).thenReturn(true);
    when(aspectMap.get(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME))
        .thenReturn(envelopedAspect);
    when(entityResponse.getAspects()).thenReturn(aspectMap);

    when(mockClient.batchGetV2(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_EXECUTION_REQUEST_URN)),
            any()))
        .thenReturn(ImmutableMap.of(TEST_EXECUTION_REQUEST_URN, entityResponse));

    when(mockS3Util.generatePresignedDownloadUrl("test-bucket", "path/to/file.tar.gz", 3600))
        .thenReturn(TEST_PRESIGNED_URL);

    GetExecutionRequestDownloadUrlResolver resolver =
        new GetExecutionRequestDownloadUrlResolver(mockClient, mockS3Util);

    // Execute
    GetExecutionRequestDownloadUrlResult result = resolver.get(mockEnv).join();

    // Verify
    assertEquals(result.getDownloadUrl(), TEST_PRESIGNED_URL);
    assertEquals((int) result.getExpiresIn(), 3600); // Default expiration
    verify(mockS3Util).generatePresignedDownloadUrl("test-bucket", "path/to/file.tar.gz", 3600);
  }

  @Test(expectedExceptions = CompletionException.class)
  public void testInvalidUrnThrowsException() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    S3Util mockS3Util = mock(S3Util.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = TestUtils.getMockAllowContext();

    GetExecutionRequestDownloadUrlInput input = new GetExecutionRequestDownloadUrlInput();
    input.setExecutionRequestUrn("urn:li:dataset:(test,test,test)"); // Wrong entity type
    input.setExpirationSeconds(3600);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    GetExecutionRequestDownloadUrlResolver resolver =
        new GetExecutionRequestDownloadUrlResolver(mockClient, mockS3Util);

    // Execute - should throw exception
    resolver.get(mockEnv).join();
  }

  @Test(expectedExceptions = CompletionException.class)
  public void testUnauthorizedUserThrowsException() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    S3Util mockS3Util = mock(S3Util.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = TestUtils.getMockDenyContext(); // Use deny context

    GetExecutionRequestDownloadUrlInput input = new GetExecutionRequestDownloadUrlInput();
    input.setExecutionRequestUrn(TEST_EXECUTION_REQUEST_URN.toString());
    input.setExpirationSeconds(3600);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    GetExecutionRequestDownloadUrlResolver resolver =
        new GetExecutionRequestDownloadUrlResolver(mockClient, mockS3Util);

    // Execute - should throw exception due to authorization failure
    resolver.get(mockEnv).join();
  }

  @Test(expectedExceptions = CompletionException.class)
  public void testExecutionRequestNotFoundThrowsException() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    S3Util mockS3Util = mock(S3Util.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = TestUtils.getMockAllowContext();

    GetExecutionRequestDownloadUrlInput input = new GetExecutionRequestDownloadUrlInput();
    input.setExecutionRequestUrn(TEST_EXECUTION_REQUEST_URN.toString());
    input.setExpirationSeconds(3600);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Return empty response (no entity found)
    when(mockClient.batchGetV2(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_EXECUTION_REQUEST_URN)),
            any()))
        .thenReturn(ImmutableMap.of());

    GetExecutionRequestDownloadUrlResolver resolver =
        new GetExecutionRequestDownloadUrlResolver(mockClient, mockS3Util);

    // Execute - should throw exception
    resolver.get(mockEnv).join();
  }

  @Test(expectedExceptions = CompletionException.class)
  public void testNoUploadLocationAspectThrowsException() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    S3Util mockS3Util = mock(S3Util.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = TestUtils.getMockAllowContext();

    GetExecutionRequestDownloadUrlInput input = new GetExecutionRequestDownloadUrlInput();
    input.setExecutionRequestUrn(TEST_EXECUTION_REQUEST_URN.toString());
    input.setExpirationSeconds(3600);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Mock entity response without upload location aspect
    EntityResponse entityResponse = mock(EntityResponse.class);
    EnvelopedAspectMap aspectMap = mock(EnvelopedAspectMap.class);
    when(aspectMap.containsKey(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME)).thenReturn(false);
    when(entityResponse.getAspects()).thenReturn(aspectMap);

    when(mockClient.batchGetV2(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_EXECUTION_REQUEST_URN)),
            any()))
        .thenReturn(ImmutableMap.of(TEST_EXECUTION_REQUEST_URN, entityResponse));

    GetExecutionRequestDownloadUrlResolver resolver =
        new GetExecutionRequestDownloadUrlResolver(mockClient, mockS3Util);

    // Execute - should throw exception
    resolver.get(mockEnv).join();
  }

  @Test(expectedExceptions = CompletionException.class)
  public void testNonS3LocationThrowsException() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    S3Util mockS3Util = mock(S3Util.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = TestUtils.getMockAllowContext();

    GetExecutionRequestDownloadUrlInput input = new GetExecutionRequestDownloadUrlInput();
    input.setExecutionRequestUrn(TEST_EXECUTION_REQUEST_URN.toString());
    input.setExpirationSeconds(3600);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Mock ExecutionRequestArtifactsLocation aspect with non-S3 location
    ExecutionRequestArtifactsLocation uploadLocation = new ExecutionRequestArtifactsLocation();
    uploadLocation.setLocation("http://example.com/file.tar.gz"); // Non-S3 location

    EnvelopedAspect envelopedAspect = mock(EnvelopedAspect.class);
    Aspect mockAspect = mock(Aspect.class);
    when(envelopedAspect.getValue()).thenReturn(mockAspect);
    when(mockAspect.data()).thenReturn(uploadLocation.data());

    EntityResponse entityResponse = mock(EntityResponse.class);
    EnvelopedAspectMap aspectMap = mock(EnvelopedAspectMap.class);
    when(aspectMap.containsKey(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME)).thenReturn(true);
    when(aspectMap.get(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME))
        .thenReturn(envelopedAspect);
    when(entityResponse.getAspects()).thenReturn(aspectMap);

    when(mockClient.batchGetV2(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_EXECUTION_REQUEST_URN)),
            any()))
        .thenReturn(ImmutableMap.of(TEST_EXECUTION_REQUEST_URN, entityResponse));

    GetExecutionRequestDownloadUrlResolver resolver =
        new GetExecutionRequestDownloadUrlResolver(mockClient, mockS3Util);

    // Execute - should throw exception
    resolver.get(mockEnv).join();
  }

  @Test(expectedExceptions = CompletionException.class)
  public void testInvalidExpirationThrowsException() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    S3Util mockS3Util = mock(S3Util.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = TestUtils.getMockAllowContext();

    GetExecutionRequestDownloadUrlInput input = new GetExecutionRequestDownloadUrlInput();
    input.setExecutionRequestUrn(TEST_EXECUTION_REQUEST_URN.toString());
    input.setExpirationSeconds(1000000); // Too large (>7 days)

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Mock ExecutionRequestArtifactsLocation aspect
    ExecutionRequestArtifactsLocation uploadLocation = new ExecutionRequestArtifactsLocation();
    uploadLocation.setLocation(TEST_S3_LOCATION);

    EnvelopedAspect envelopedAspect = mock(EnvelopedAspect.class);
    Aspect mockAspect = mock(Aspect.class);
    when(envelopedAspect.getValue()).thenReturn(mockAspect);
    when(mockAspect.data()).thenReturn(uploadLocation.data());

    EntityResponse entityResponse = mock(EntityResponse.class);
    EnvelopedAspectMap aspectMap = mock(EnvelopedAspectMap.class);
    when(aspectMap.containsKey(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME)).thenReturn(true);
    when(aspectMap.get(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME))
        .thenReturn(envelopedAspect);
    when(entityResponse.getAspects()).thenReturn(aspectMap);

    when(mockClient.batchGetV2(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_EXECUTION_REQUEST_URN)),
            any()))
        .thenReturn(ImmutableMap.of(TEST_EXECUTION_REQUEST_URN, entityResponse));

    GetExecutionRequestDownloadUrlResolver resolver =
        new GetExecutionRequestDownloadUrlResolver(mockClient, mockS3Util);

    // Execute - should throw exception
    resolver.get(mockEnv).join();
  }
}
