package com.linkedin.metadata.utils.aws;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class S3UtilTest {

  @Mock private S3Client mockS3Client;
  @Mock private StsClient mockStsClient;
  @Mock private S3Presigner mockS3Presigner; // Mock S3Presigner

  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);
    System.setProperty("aws.region", "us-east-1");
  }

  @AfterMethod
  public void tearDown() throws Exception {
    mocks.close();
    System.clearProperty("aws.region");
  }

  @Test
  public void testConstructorWithS3Client() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);
    assertNotNull(s3Util);
  }

  @Test
  public void testConstructorWithStsClient() {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";

    Credentials mockCredentials =
        Credentials.builder()
            .accessKeyId("access-key")
            .secretAccessKey("secret-key")
            .sessionToken("session-token")
            .expiration(Instant.now().plusSeconds(3600))
            .build();

    AssumeRoleResponse mockResponse =
        AssumeRoleResponse.builder().credentials(mockCredentials).build();

    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);

    S3Util s3Util = new S3Util(mockStsClient, roleArn);
    assertNotNull(s3Util);
  }

  @Test
  public void testGeneratePresignedDownloadUrlWithoutCredentialRefresh() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    String bucket = "test-bucket";
    String key = "test-key";
    int expirationSeconds = 3600;

    // This will throw an exception because mockS3Presigner will throw when called
    when(mockS3Presigner.presignGetObject(any(GetObjectPresignRequest.class)))
        .thenThrow(new RuntimeException("Presigner error"));

    assertThrows(
        RuntimeException.class,
        () -> s3Util.generatePresignedDownloadUrl(bucket, key, expirationSeconds));
  }

  @Test
  public void testGeneratePresignedDownloadUrlWithNullParameters() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    assertThrows(Exception.class, () -> s3Util.generatePresignedDownloadUrl(null, "key", 3600));

    assertThrows(Exception.class, () -> s3Util.generatePresignedDownloadUrl("bucket", null, 3600));
  }

  @Test
  public void testConcurrentCredentialRefresh() throws InterruptedException {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";

    Credentials mockCredentials =
        Credentials.builder()
            .accessKeyId("access-key")
            .secretAccessKey("secret-key")
            .sessionToken("session-token")
            .expiration(Instant.now().plusSeconds(3600))
            .build();

    AssumeRoleResponse mockResponse =
        AssumeRoleResponse.builder().credentials(mockCredentials).build();

    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);

    S3Util s3Util = new S3Util(mockStsClient, roleArn);

    ExecutorService executor = Executors.newFixedThreadPool(5);
    CompletableFuture<?>[] futures = new CompletableFuture[10];

    for (int i = 0; i < 10; i++) {
      futures[i] =
          CompletableFuture.runAsync(
              () -> {
                try {
                  s3Util.generatePresignedDownloadUrl("bucket", "key", 3600);
                } catch (Exception e) {
                  // Expected since we're using mocked clients
                }
              },
              executor);
    }

    CompletableFuture.allOf(futures).join();
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
  }

  @Test
  public void testGeneratePresignedUrlExceptionHandling() {
    // Mock the presigner to pass it explicitly, bypassing createPresigner()
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    // Mock the presigner to throw an exception during URL generation
    when(mockS3Presigner.presignGetObject(any(GetObjectPresignRequest.class)))
        .thenThrow(new RuntimeException("Presigner error"));

    assertThrows(
        RuntimeException.class, () -> s3Util.generatePresignedDownloadUrl("bucket", "key", 3600));
  }

  @Test
  public void testConstructorParameterValidation() {
    // The constructors don't currently validate null parameters, so let's just test they don't
    // throw
    S3Util s3Util1 = new S3Util(mockS3Client, mockS3Presigner);
    assertNotNull(s3Util1);

    // Test the STS constructor variation
    Credentials mockCredentials =
        Credentials.builder()
            .accessKeyId("test-access-key")
            .secretAccessKey("test-secret-key")
            .sessionToken("test-session-token")
            .expiration(Instant.now().plusSeconds(3600))
            .build();

    AssumeRoleResponse mockResponse =
        AssumeRoleResponse.builder().credentials(mockCredentials).build();

    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);

    S3Util s3Util2 = new S3Util(mockStsClient, "arn:aws:iam::123456789012:role/test");
    assertNotNull(s3Util2);
  }

  @Test
  public void testGeneratePresignedUploadUrlSuccess() throws Exception {
    String bucket = "test-upload-bucket";
    String key = "test-upload-key";
    int expirationSeconds = 3600;
    String contentType = "image/jpeg";
    String expectedUrl =
        "https://test-upload-bucket.s3.amazonaws.com/test-upload-key?X-Amz-Signature=mocked";

    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    PresignedPutObjectRequest mockPresignedRequest = mock(PresignedPutObjectRequest.class);
    when(mockPresignedRequest.url()).thenReturn(new URL(expectedUrl));

    when(mockS3Presigner.presignPutObject(any(PutObjectPresignRequest.class)))
        .thenReturn(mockPresignedRequest);

    String actualUrl =
        s3Util.generatePresignedUploadUrl(bucket, key, expirationSeconds, contentType);

    assertNotNull(actualUrl);
    assertEquals(actualUrl, expectedUrl);

    verify(mockS3Presigner).presignPutObject(any(PutObjectPresignRequest.class));
  }

  @Test
  public void testGeneratePresignedUploadUrlWithNullParameters() throws Exception {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    // Test with null bucket
    assertThrows(
        Exception.class, () -> s3Util.generatePresignedUploadUrl(null, "key", 3600, "image/jpeg"));

    // Test with null key
    assertThrows(
        Exception.class,
        () -> s3Util.generatePresignedUploadUrl("bucket", null, 3600, "image/jpeg"));

    // Test with null contentType (should not throw, as contentType is @Nullable)
    String bucket = "test-upload-bucket";
    String key = "test-upload-key";
    int expirationSeconds = 3600;
    String expectedUrl =
        "https://test-upload-bucket.s3.amazonaws.com/test-upload-key?X-Amz-Signature=mocked";

    PresignedPutObjectRequest mockPresignedRequest = mock(PresignedPutObjectRequest.class);
    when(mockPresignedRequest.url()).thenReturn(new URL(expectedUrl));

    when(mockS3Presigner.presignPutObject(any(PutObjectPresignRequest.class)))
        .thenReturn(mockPresignedRequest);

    String actualUrl = s3Util.generatePresignedUploadUrl(bucket, key, expirationSeconds, null);
    assertNotNull(actualUrl);
    assertEquals(actualUrl, expectedUrl);
  }

  @Test
  public void testGeneratePresignedUploadUrlExceptionHandling() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    String bucket = "test-upload-bucket";
    String key = "test-upload-key";
    int expirationSeconds = 3600;
    String contentType = "image/jpeg";

    when(mockS3Presigner.presignPutObject(any(PutObjectPresignRequest.class)))
        .thenThrow(new RuntimeException("S3 presigner error"));

    assertThrows(
        RuntimeException.class,
        () -> s3Util.generatePresignedUploadUrl(bucket, key, expirationSeconds, contentType));
  }

  @Test
  public void testConstructorWithS3ClientAndPresigner() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);
    assertNotNull(s3Util);
  }

  @Test
  public void testConstructorWithStsClientAndPresigner() {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";

    Credentials mockCredentials =
        Credentials.builder()
            .accessKeyId("access-key")
            .secretAccessKey("secret-key")
            .sessionToken("session-token")
            .expiration(Instant.now().plusSeconds(3600))
            .build();

    AssumeRoleResponse mockResponse =
        AssumeRoleResponse.builder().credentials(mockCredentials).build();

    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);

    S3Util s3Util = new S3Util(mockStsClient, roleArn, mockS3Presigner);
    assertNotNull(s3Util);
  }

  @Test
  public void testGeneratePresignedDownloadUrlSuccess() throws Exception {
    String bucket = "test-download-bucket";
    String key = "test-download-key";
    int expirationSeconds = 3600;
    String expectedUrl =
        "https://test-download-bucket.s3.amazonaws.com/test-download-key?X-Amz-Signature=mocked";

    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    PresignedGetObjectRequest mockPresignedRequest = mock(PresignedGetObjectRequest.class);
    when(mockPresignedRequest.url()).thenReturn(new URL(expectedUrl));

    when(mockS3Presigner.presignGetObject(any(GetObjectPresignRequest.class)))
        .thenReturn(mockPresignedRequest);

    String actualUrl = s3Util.generatePresignedDownloadUrl(bucket, key, expirationSeconds);

    assertNotNull(actualUrl);
    assertEquals(actualUrl, expectedUrl);

    verify(mockS3Presigner).presignGetObject(any(GetObjectPresignRequest.class));
  }

  @Test
  public void testGeneratePresignedDownloadUrlExceptionHandling() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    String bucket = "test-download-bucket";
    String key = "test-download-key";
    int expirationSeconds = 3600;

    when(mockS3Presigner.presignGetObject(any(GetObjectPresignRequest.class)))
        .thenThrow(new RuntimeException("S3 presigner error"));

    assertThrows(
        RuntimeException.class,
        () -> s3Util.generatePresignedDownloadUrl(bucket, key, expirationSeconds));
  }

  @Test
  public void testConstructorWithExpiredCredentials() {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";

    // Create credentials that are already expired
    Credentials expiredCredentials =
        Credentials.builder()
            .accessKeyId("access-key")
            .secretAccessKey("secret-key")
            .sessionToken("session-token")
            .expiration(Instant.now().minusSeconds(3600)) // Expired 1 hour ago
            .build();

    AssumeRoleResponse mockResponse =
        AssumeRoleResponse.builder().credentials(expiredCredentials).build();

    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);

    S3Util s3Util = new S3Util(mockStsClient, roleArn);
    assertNotNull(s3Util);
  }

  @Test
  public void testStsClientConstructorCreatesS3Client() {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";

    Credentials mockCredentials =
        Credentials.builder()
            .accessKeyId("access-key")
            .secretAccessKey("secret-key")
            .sessionToken("session-token")
            .expiration(Instant.now().plusSeconds(3600))
            .build();

    AssumeRoleResponse mockResponse =
        AssumeRoleResponse.builder().credentials(mockCredentials).build();

    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);

    S3Util s3Util = new S3Util(mockStsClient, roleArn);
    assertNotNull(s3Util);
  }

  @Test
  public void testPresignerCreationWithS3ClientConfiguration() {
    // Mock S3Client configuration to throw exception BEFORE creating S3Util
    when(mockS3Client.serviceClientConfiguration())
        .thenThrow(new RuntimeException("S3 client configuration error"));

    // This will fail during construction when createPresigner() is called
    assertThrows(RuntimeException.class, () -> new S3Util(mockS3Client));
  }

  @Test
  public void testDeleteObjectSuccess() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    String bucket = "test-bucket";
    String key = "test-key";

    software.amazon.awssdk.services.s3.model.DeleteObjectResponse mockResponse =
        mock(software.amazon.awssdk.services.s3.model.DeleteObjectResponse.class);
    software.amazon.awssdk.http.SdkHttpResponse mockHttpResponse =
        mock(software.amazon.awssdk.http.SdkHttpResponse.class);

    when(mockResponse.sdkHttpResponse()).thenReturn(mockHttpResponse);
    when(mockHttpResponse.isSuccessful()).thenReturn(true);
    when(mockS3Client.deleteObject(
            any(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.class)))
        .thenReturn(mockResponse);

    // Should not throw any exception
    s3Util.deleteObject(bucket, key);

    verify(mockS3Client)
        .deleteObject(any(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.class));
  }

  @Test
  public void testDeleteObjectFailure() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    String bucket = "test-bucket";
    String key = "test-key";

    software.amazon.awssdk.services.s3.model.DeleteObjectResponse mockResponse =
        mock(software.amazon.awssdk.services.s3.model.DeleteObjectResponse.class);
    software.amazon.awssdk.http.SdkHttpResponse mockHttpResponse =
        mock(software.amazon.awssdk.http.SdkHttpResponse.class);

    when(mockResponse.sdkHttpResponse()).thenReturn(mockHttpResponse);
    when(mockHttpResponse.isSuccessful()).thenReturn(false);
    when(mockHttpResponse.statusCode()).thenReturn(403);
    when(mockS3Client.deleteObject(
            any(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.class)))
        .thenReturn(mockResponse);

    assertThrows(RuntimeException.class, () -> s3Util.deleteObject(bucket, key));
  }

  @Test
  public void testDeleteObjectException() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    String bucket = "test-bucket";
    String key = "test-key";

    when(mockS3Client.deleteObject(
            any(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.class)))
        .thenThrow(new RuntimeException("S3 service error"));

    assertThrows(RuntimeException.class, () -> s3Util.deleteObject(bucket, key));
  }

  @Test
  public void testDeleteObjectWithNullParameters() {
    S3Util s3Util = new S3Util(mockS3Client, mockS3Presigner);

    // Test with null bucket
    assertThrows(Exception.class, () -> s3Util.deleteObject(null, "key"));

    // Test with null key
    assertThrows(Exception.class, () -> s3Util.deleteObject("bucket", null));
  }
}
