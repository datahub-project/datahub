package com.linkedin.datahub.graphql.resolvers.files;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrl;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlInput;
import com.linkedin.datahub.graphql.generated.UploadDownloadScenario;
import com.linkedin.datahub.graphql.util.S3Util;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetPresignedUploadUrlResolverTest {

  private static final String TEST_BUCKET_NAME = "my-test-bucket";
  private static final String TEST_ASSET_URN = "urn:li:dataPlatform:test:testAsset";
  private static final String TEST_CONTENT_TYPE = "image/png";
  private static final String MOCKED_PRESIGNED_URL = "https://mocked.s3.url/test-key";

  @Mock private S3Util mockS3Util;
  @Mock private QueryContext mockQueryContext;
  @Mock private DataFetchingEnvironment mockEnv;
  @Mock private Authorizer mockAuthorizer;

  private AutoCloseable mocks;
  private MockedStatic<AuthorizationUtils> authorizationUtilsMockedStatic;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);
    // Mock QueryContext to return a mocked Authorizer
    when(mockQueryContext.getAuthorizer()).thenReturn(mockAuthorizer);
    // Mock Authorizer to always return an ALLOWED result for any authorization request
    when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, ""));

    // Mock static method AuthorizationUtils.isAuthorized
    authorizationUtilsMockedStatic = mockStatic(AuthorizationUtils.class);
    authorizationUtilsMockedStatic
        .when(
            () ->
                AuthorizationUtils.isAuthorized(
                    any(QueryContext.class),
                    any(String.class),
                    any(String.class),
                    any(DisjunctivePrivilegeGroup.class)))
        .thenReturn(true);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    mocks.close();
    authorizationUtilsMockedStatic.close();
  }

  private GetPresignedUploadUrlInput createInput(
      UploadDownloadScenario scenario, String assetUrn, String contentType) {
    GetPresignedUploadUrlInput input = new GetPresignedUploadUrlInput();
    input.setScenario(scenario);
    input.setAssetUrn(assetUrn);
    input.setContentType(contentType);
    return input;
  }

  @Test
  public void testGetPresignedUploadUrlWithNullS3Util() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(UploadDownloadScenario.ASSET_DOCUMENTATION, TEST_ASSET_URN, TEST_CONTENT_TYPE);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(null, TEST_BUCKET_NAME);
    assertThrows(
        "S3Util isn't provided", IllegalArgumentException.class, () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithBucketName() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(UploadDownloadScenario.ASSET_DOCUMENTATION, TEST_ASSET_URN, TEST_CONTENT_TYPE);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME), any(String.class), anyInt(), eq(TEST_CONTENT_TYPE)))
        .thenReturn(MOCKED_PRESIGNED_URL);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, TEST_BUCKET_NAME);
    CompletableFuture<GetPresignedUploadUrl> future = resolver.get(mockEnv);
    GetPresignedUploadUrl result = future.get();

    assertNotNull(result);
    assertEquals(result.getUrl(), MOCKED_PRESIGNED_URL);

    verify(mockS3Util)
        .generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME), any(String.class), anyInt(), eq(TEST_CONTENT_TYPE));
  }

  @Test
  public void testGetPresignedUploadUrlWithNullBucketName() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(UploadDownloadScenario.ASSET_DOCUMENTATION, TEST_ASSET_URN, TEST_CONTENT_TYPE);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    GetPresignedUploadUrlResolver resolver = new GetPresignedUploadUrlResolver(mockS3Util, null);
    assertThrows(
        "Bucket name isn't provided",
        IllegalArgumentException.class,
        () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithEmptyBucketName() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(UploadDownloadScenario.ASSET_DOCUMENTATION, TEST_ASSET_URN, TEST_CONTENT_TYPE);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    GetPresignedUploadUrlResolver resolver = new GetPresignedUploadUrlResolver(mockS3Util, "");
    assertThrows(
        "Bucket name isn't provided",
        IllegalArgumentException.class,
        () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithNullAssetUrnForAssetDocumentation() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(UploadDownloadScenario.ASSET_DOCUMENTATION, null, TEST_CONTENT_TYPE);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, TEST_BUCKET_NAME);
    assertThrows(
        "assetUrn is required for ASSET_DOCUMENTATION scenario",
        IllegalArgumentException.class,
        () -> resolver.get(mockEnv).get());
  }
}
