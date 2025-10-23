package com.linkedin.datahub.graphql.resolvers.files;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlInput;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlResponse;
import com.linkedin.datahub.graphql.generated.UploadDownloadScenario;
import com.linkedin.datahub.graphql.resolvers.mutate.DescriptionUtils;
import com.linkedin.metadata.config.S3Configuration;
import com.linkedin.metadata.utils.aws.S3Util;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
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
  private static final Integer TEST_EXPIRATION_SECONDS = 3600; // Default from application.yaml
  private static final String TEST_ASSET_PATH_PREFIX =
      "product-assets"; // Default from application.yaml

  @Mock private S3Util mockS3Util;
  @Mock private QueryContext mockQueryContext;
  @Mock private DataFetchingEnvironment mockEnv;
  @Mock private Authorizer mockAuthorizer;
  @Mock private S3Configuration mockS3Configuration;

  private AutoCloseable mocks;
  private MockedStatic<DescriptionUtils> descriptionUtilsMockedStatic;

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

    descriptionUtilsMockedStatic = mockStatic(DescriptionUtils.class);
    descriptionUtilsMockedStatic
        .when(
            () ->
                DescriptionUtils.isAuthorizedToUpdateDescription(
                    any(QueryContext.class), any(Urn.class)))
        .thenReturn(true);
    descriptionUtilsMockedStatic
        .when(
            () ->
                DescriptionUtils.isAuthorizedToUpdateFieldDescription(
                    any(QueryContext.class), any(Urn.class)))
        .thenReturn(true);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    mocks.close();
    descriptionUtilsMockedStatic.close();
  }

  private GetPresignedUploadUrlInput createInput(
      UploadDownloadScenario scenario,
      String assetUrn,
      String schemaFieldUrn,
      String contentType,
      String fileName) {
    GetPresignedUploadUrlInput input = new GetPresignedUploadUrlInput();
    input.setScenario(scenario);
    input.setAssetUrn(assetUrn);
    input.setSchemaFieldUrn(schemaFieldUrn);
    input.setContentType(contentType);
    input.setFileName(fileName);
    return input;
  }

  //   private GetPresignedUploadUrlInput createInput(
  //       UploadDownloadScenario scenario, String assetUrn, String contentType, String fileName) {
  //     return createInput(scenario, assetUrn, null, contentType, fileName);
  //   }

  @Test
  public void testGetPresignedUploadUrlReturnsFileId() throws Exception {
    String testFileName = "my_test_file.pdf";
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            testFileName);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    ArgumentCaptor<String> s3KeyCaptor = ArgumentCaptor.forClass(String.class);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            s3KeyCaptor.capture(),
            eq(TEST_EXPIRATION_SECONDS),
            eq(TEST_CONTENT_TYPE)))
        .thenReturn(MOCKED_PRESIGNED_URL);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    CompletableFuture<GetPresignedUploadUrlResponse> future = resolver.get(mockEnv);
    GetPresignedUploadUrlResponse result = future.get();

    assertNotNull(result);
    assertEquals(result.getUrl(), MOCKED_PRESIGNED_URL);
    assertNotNull(result.getFileId());

    String capturedS3Key = s3KeyCaptor.getValue();
    assertTrue(capturedS3Key.startsWith(TEST_BUCKET_NAME + "/" + TEST_ASSET_PATH_PREFIX + "/"));

    // Extract fileId from s3Key
    String expectedFileIdPrefix = TEST_BUCKET_NAME + "/" + TEST_ASSET_PATH_PREFIX + "/";
    String extractedFileId = capturedS3Key.substring(expectedFileIdPrefix.length());

    assertEquals(result.getFileId(), extractedFileId);
    assertTrue(result.getFileId().contains(testFileName));
  }

  @Test
  public void testGetPresignedUploadUrlGeneratesCorrectS3Key() throws Exception {
    String testFileName = "my_document.pdf";
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            testFileName);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    ArgumentCaptor<String> s3KeyCaptor = ArgumentCaptor.forClass(String.class);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            s3KeyCaptor.capture(),
            eq(TEST_EXPIRATION_SECONDS),
            eq(TEST_CONTENT_TYPE)))
        .thenReturn(MOCKED_PRESIGNED_URL);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    CompletableFuture<GetPresignedUploadUrlResponse> future = resolver.get(mockEnv);
    future.get(); // Execute the resolver to capture the argument

    String capturedS3Key = s3KeyCaptor.getValue();
    assertTrue(capturedS3Key.startsWith(TEST_BUCKET_NAME + "/" + TEST_ASSET_PATH_PREFIX + "/"));
    assertTrue(capturedS3Key.contains(testFileName));
  }

  @Test
  public void testGetPresignedUploadUrlWithNullS3Util() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            "test.png");

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(null, mockS3Configuration);
    assertThrows(
        "S3Util isn't provided", IllegalArgumentException.class, () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithBucketName() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            "test.png");

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            any(String.class),
            eq(TEST_EXPIRATION_SECONDS),
            eq(TEST_CONTENT_TYPE)))
        .thenReturn(MOCKED_PRESIGNED_URL);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    CompletableFuture<GetPresignedUploadUrlResponse> future = resolver.get(mockEnv);
    GetPresignedUploadUrlResponse result = future.get();

    assertNotNull(result);
    assertEquals(result.getUrl(), MOCKED_PRESIGNED_URL);

    verify(mockS3Util)
        .generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            any(String.class),
            eq(TEST_EXPIRATION_SECONDS),
            eq(TEST_CONTENT_TYPE));
  }

  @Test
  public void testGetPresignedUploadUrlWithNullBucketName() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            "test.png");

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    when(mockS3Configuration.getBucketName()).thenReturn(null); // Simulate null bucket name
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    assertThrows(
        "Bucket name isn't provided",
        IllegalArgumentException.class,
        () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithEmptyBucketName() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            "test.png");

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    when(mockS3Configuration.getBucketName()).thenReturn(""); // Simulate empty bucket name
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    assertThrows(
        "Bucket name isn't provided",
        IllegalArgumentException.class,
        () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithNullAssetUrnForAssetDocumentation() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION, null, null, TEST_CONTENT_TYPE, "test.png");

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    assertThrows(
        "assetUrn is required for ASSET_DOCUMENTATION scenario",
        IllegalArgumentException.class,
        () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithAuthorizationFailure() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            "test.png");

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    // Mock asset description authorization to return false (for when schemaFieldUrn is null)
    descriptionUtilsMockedStatic
        .when(
            () ->
                DescriptionUtils.isAuthorizedToUpdateDescription(
                    any(QueryContext.class), any(Urn.class)))
        .thenReturn(false);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    assertThrows(
        "Unauthorized to edit documentation for asset: " + TEST_ASSET_URN,
        AuthorizationException.class,
        () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithUnsupportedScenario() throws Exception {
    // Since only ASSET_DOCUMENTATION is supported, we'll test the else branch in getS3Key
    // by creating a mock scenario that's not ASSET_DOCUMENTATION
    GetPresignedUploadUrlInput input = new GetPresignedUploadUrlInput();
    input.setScenario(null); // Set to null to trigger the else branch
    input.setAssetUrn(TEST_ASSET_URN);
    input.setSchemaFieldUrn(null);
    input.setContentType(TEST_CONTENT_TYPE);
    input.setFileName("test.png");

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlGeneratesUniqueFileIds() throws Exception {
    GetPresignedUploadUrlInput input1 =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            "test1.pdf");
    GetPresignedUploadUrlInput input2 =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            "test2.pdf");

    when(mockEnv.getArgument("input")).thenReturn(input1).thenReturn(input2);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            any(String.class),
            eq(TEST_EXPIRATION_SECONDS),
            eq(TEST_CONTENT_TYPE)))
        .thenReturn(MOCKED_PRESIGNED_URL);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);

    CompletableFuture<GetPresignedUploadUrlResponse> future1 = resolver.get(mockEnv);
    GetPresignedUploadUrlResponse result1 = future1.get();

    CompletableFuture<GetPresignedUploadUrlResponse> future2 = resolver.get(mockEnv);
    GetPresignedUploadUrlResponse result2 = future2.get();

    assertNotNull(result1.getFileId());
    assertNotNull(result2.getFileId());
    assertNotEquals(result1.getFileId(), result2.getFileId(), "File IDs should be unique");
  }

  @Test
  public void testGetPresignedUploadUrlWithDifferentContentTypes() throws Exception {
    String[] contentTypes = {
      "image/png",
      "image/jpeg",
      "application/pdf",
      "text/plain",
      "application/vnd.ms-excel",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    };

    for (String contentType : contentTypes) {
      GetPresignedUploadUrlInput input =
          createInput(
              UploadDownloadScenario.ASSET_DOCUMENTATION,
              TEST_ASSET_URN,
              null,
              contentType,
              "test.pdf");

      when(mockEnv.getArgument("input")).thenReturn(input);
      when(mockEnv.getContext()).thenReturn(mockQueryContext);
      when(mockS3Util.generatePresignedUploadUrl(
              eq(TEST_BUCKET_NAME),
              any(String.class),
              eq(TEST_EXPIRATION_SECONDS),
              eq(contentType)))
          .thenReturn(MOCKED_PRESIGNED_URL);

      when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
      when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
          .thenReturn(TEST_EXPIRATION_SECONDS);
      when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

      GetPresignedUploadUrlResolver resolver =
          new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
      CompletableFuture<GetPresignedUploadUrlResponse> future = resolver.get(mockEnv);
      GetPresignedUploadUrlResponse result = future.get();

      assertNotNull(result, "Result should not be null for content type: " + contentType);
      assertEquals(result.getUrl(), MOCKED_PRESIGNED_URL);

      // Verify that the correct content type was passed to S3Util
      verify(mockS3Util)
          .generatePresignedUploadUrl(
              eq(TEST_BUCKET_NAME),
              any(String.class),
              eq(TEST_EXPIRATION_SECONDS),
              eq(contentType));
    }
  }

  @Test
  public void testGetPresignedUploadUrlWithNullContentType() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            null, // null content type
            "test.pdf");

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            any(String.class),
            eq(TEST_EXPIRATION_SECONDS),
            eq((String) null)))
        .thenReturn(MOCKED_PRESIGNED_URL);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    CompletableFuture<GetPresignedUploadUrlResponse> future = resolver.get(mockEnv);
    GetPresignedUploadUrlResponse result = future.get();

    assertNotNull(result);
    assertEquals(result.getUrl(), MOCKED_PRESIGNED_URL);

    // Verify that null content type was passed to S3Util
    verify(mockS3Util)
        .generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            any(String.class),
            eq(TEST_EXPIRATION_SECONDS),
            eq((String) null));
  }

  @Test
  public void testGetPresignedUploadUrlWithS3UtilException() throws Exception {
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null,
            TEST_CONTENT_TYPE,
            "test.pdf");

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            any(String.class),
            eq(TEST_EXPIRATION_SECONDS),
            eq(TEST_CONTENT_TYPE)))
        .thenThrow(new RuntimeException("S3 service unavailable"));

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);

    // The RuntimeException gets wrapped in ExecutionException when called via
    // CompletableFuture.get()
    assertThrows(java.util.concurrent.ExecutionException.class, () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithSchemaFieldUrn() throws Exception {
    String testFileName = "my_test_file.pdf";
    String schemaFieldUrn =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my-dataset,PROD),myField)";
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            schemaFieldUrn,
            TEST_CONTENT_TYPE,
            testFileName);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    ArgumentCaptor<String> s3KeyCaptor = ArgumentCaptor.forClass(String.class);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            s3KeyCaptor.capture(),
            eq(TEST_EXPIRATION_SECONDS),
            eq(TEST_CONTENT_TYPE)))
        .thenReturn(MOCKED_PRESIGNED_URL);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    CompletableFuture<GetPresignedUploadUrlResponse> future = resolver.get(mockEnv);
    GetPresignedUploadUrlResponse result = future.get();

    // Verify that the schema field authorization is called when schemaFieldUrn is present
    descriptionUtilsMockedStatic.verify(
        () ->
            DescriptionUtils.isAuthorizedToUpdateFieldDescription(
                any(QueryContext.class), any(Urn.class)));

    assertNotNull(result);
    assertEquals(result.getUrl(), MOCKED_PRESIGNED_URL);
    assertNotNull(result.getFileId());

    String capturedS3Key = s3KeyCaptor.getValue();
    assertTrue(capturedS3Key.startsWith(TEST_BUCKET_NAME + "/" + TEST_ASSET_PATH_PREFIX + "/"));

    // Extract fileId from s3Key
    String expectedFileIdPrefix = TEST_BUCKET_NAME + "/" + TEST_ASSET_PATH_PREFIX + "/";
    String extractedFileId = capturedS3Key.substring(expectedFileIdPrefix.length());

    assertEquals(result.getFileId(), extractedFileId);
    assertTrue(result.getFileId().contains(testFileName));
  }

  @Test
  public void testGetPresignedUploadUrlWithSchemaFieldUrnAuthorizationFailure() throws Exception {
    String testFileName = "my_test_file.pdf";
    String schemaFieldUrn =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my-dataset,PROD),myField)";
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            schemaFieldUrn,
            TEST_CONTENT_TYPE,
            testFileName);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    // Mock field authorization to return false
    descriptionUtilsMockedStatic
        .when(
            () ->
                DescriptionUtils.isAuthorizedToUpdateFieldDescription(
                    any(QueryContext.class), any(Urn.class)))
        .thenReturn(false);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    assertThrows(
        "Unauthorized to edit documentation for schema field: " + schemaFieldUrn,
        AuthorizationException.class,
        () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPresignedUploadUrlWithAssetUrnAndNullSchemaFieldUrn() throws Exception {
    String testFileName = "my_test_file.pdf";
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN,
            null, // schemaFieldUrn is null, so use assetUrn authorization
            TEST_CONTENT_TYPE,
            testFileName);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    ArgumentCaptor<String> s3KeyCaptor = ArgumentCaptor.forClass(String.class);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            s3KeyCaptor.capture(),
            eq(TEST_EXPIRATION_SECONDS),
            eq(TEST_CONTENT_TYPE)))
        .thenReturn(MOCKED_PRESIGNED_URL);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    CompletableFuture<GetPresignedUploadUrlResponse> future = resolver.get(mockEnv);
    GetPresignedUploadUrlResponse result = future.get();

    // Verify that the standard asset authorization is called when schemaFieldUrn is null
    descriptionUtilsMockedStatic.verify(
        () ->
            DescriptionUtils.isAuthorizedToUpdateDescription(
                any(QueryContext.class), any(Urn.class)));

    assertNotNull(result);
    assertEquals(result.getUrl(), MOCKED_PRESIGNED_URL);
    assertNotNull(result.getFileId());

    String capturedS3Key = s3KeyCaptor.getValue();
    assertTrue(capturedS3Key.startsWith(TEST_BUCKET_NAME + "/" + TEST_ASSET_PATH_PREFIX + "/"));

    // Extract fileId from s3Key
    String expectedFileIdPrefix = TEST_BUCKET_NAME + "/" + TEST_ASSET_PATH_PREFIX + "/";
    String extractedFileId = capturedS3Key.substring(expectedFileIdPrefix.length());

    assertEquals(result.getFileId(), extractedFileId);
    assertTrue(result.getFileId().contains(testFileName));
  }

  @Test
  public void testGetPresignedUploadUrlWhenBothAssetUrnAndSchemaFieldUrnAreProvided()
      throws Exception {
    String testFileName = "my_test_file.pdf";
    String schemaFieldUrn =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my-dataset,PROD),myField)";
    GetPresignedUploadUrlInput input =
        createInput(
            UploadDownloadScenario.ASSET_DOCUMENTATION,
            TEST_ASSET_URN, // Both assetUrn and schemaFieldUrn provided
            schemaFieldUrn,
            TEST_CONTENT_TYPE,
            testFileName);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockQueryContext);

    ArgumentCaptor<String> s3KeyCaptor = ArgumentCaptor.forClass(String.class);
    when(mockS3Util.generatePresignedUploadUrl(
            eq(TEST_BUCKET_NAME),
            s3KeyCaptor.capture(),
            eq(TEST_EXPIRATION_SECONDS),
            eq(TEST_CONTENT_TYPE)))
        .thenReturn(MOCKED_PRESIGNED_URL);

    when(mockS3Configuration.getBucketName()).thenReturn(TEST_BUCKET_NAME);
    when(mockS3Configuration.getPresignedUploadUrlExpirationSeconds())
        .thenReturn(TEST_EXPIRATION_SECONDS);
    when(mockS3Configuration.getAssetPathPrefix()).thenReturn(TEST_ASSET_PATH_PREFIX);

    // Mock only the field description authorization method to be called when schemaFieldUrn is
    // present
    descriptionUtilsMockedStatic
        .when(
            () ->
                DescriptionUtils.isAuthorizedToUpdateDescription(
                    any(QueryContext.class), any(Urn.class)))
        .thenReturn(true);
    descriptionUtilsMockedStatic
        .when(
            () ->
                DescriptionUtils.isAuthorizedToUpdateFieldDescription(
                    any(QueryContext.class), any(Urn.class)))
        .thenReturn(true);

    GetPresignedUploadUrlResolver resolver =
        new GetPresignedUploadUrlResolver(mockS3Util, mockS3Configuration);
    CompletableFuture<GetPresignedUploadUrlResponse> future = resolver.get(mockEnv);
    GetPresignedUploadUrlResponse result = future.get();

    // Verify that when schemaFieldUrn is provided, the schema field authorization is used
    // FYI: DescriptionUtils.isAuthorizedToUpdateDescription should NOT be called when
    // schemaFieldUrn is present
    descriptionUtilsMockedStatic.verify(
        () ->
            DescriptionUtils.isAuthorizedToUpdateFieldDescription(
                any(QueryContext.class), any(Urn.class)));

    assertNotNull(result);
    assertEquals(result.getUrl(), MOCKED_PRESIGNED_URL);
    assertNotNull(result.getFileId());

    String capturedS3Key = s3KeyCaptor.getValue();
    assertTrue(capturedS3Key.startsWith(TEST_BUCKET_NAME + "/" + TEST_ASSET_PATH_PREFIX + "/"));

    // Extract fileId from s3Key
    String expectedFileIdPrefix = TEST_BUCKET_NAME + "/" + TEST_ASSET_PATH_PREFIX + "/";
    String extractedFileId = capturedS3Key.substring(expectedFileIdPrefix.length());

    assertEquals(result.getFileId(), extractedFileId);
    assertTrue(result.getFileId().contains(testFileName));
  }
}
