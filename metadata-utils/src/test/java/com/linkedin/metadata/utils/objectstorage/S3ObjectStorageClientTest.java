package com.linkedin.metadata.utils.objectstorage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import java.net.URL;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

public class S3ObjectStorageClientTest {

  private static final String TEST_BUCKET = "test-bucket";

  private static S3ObjectStorageClient newClient(
      S3Client s3Client,
      S3Presigner s3Presigner,
      String bucket,
      String pathPrefix,
      int multipartThresholdBytes,
      int multipartPartSizeBytes) {
    return new S3ObjectStorageClient(
        s3Client, s3Presigner, bucket, pathPrefix, multipartThresholdBytes, multipartPartSizeBytes);
  }

  private static S3ObjectStorageClient newClient(
      S3Client s3Client,
      String bucket,
      String pathPrefix,
      int multipartThresholdBytes,
      int multipartPartSizeBytes) {
    return newClient(
        s3Client,
        mock(S3Presigner.class),
        bucket,
        pathPrefix,
        multipartThresholdBytes,
        multipartPartSizeBytes);
  }

  @Test
  public void testPutObjectAtBucketRootWhenNoPrefix() {
    S3Client s3Client = mock(S3Client.class);
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().build());

    S3ObjectStorageClient client = newClient(s3Client, TEST_BUCKET, null, 1024, 512);
    client.putObject("exports/out.bin", new byte[] {1, 2, 3});

    ArgumentCaptor<PutObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));
    assertEquals(requestCaptor.getValue().bucket(), TEST_BUCKET);
    assertEquals(requestCaptor.getValue().key(), "exports/out.bin");
  }

  @Test
  public void testPutObjectSinglePart() {
    S3Client s3Client = mock(S3Client.class);
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().build());

    S3ObjectStorageClient client = newClient(s3Client, TEST_BUCKET, "prefix", 1024, 512);
    client.putObject("exports/out.bin", new byte[] {1, 2, 3});

    ArgumentCaptor<PutObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));
    assertEquals(requestCaptor.getValue().bucket(), TEST_BUCKET);
    assertEquals(requestCaptor.getValue().key(), "prefix/exports/out.bin");
    verify(s3Client, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
  }

  @Test
  public void testPutObjectMultipart() {
    S3Client s3Client = mock(S3Client.class);
    when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
    when(s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
        .thenReturn(UploadPartResponse.builder().eTag("etag-1").build());
    when(s3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenReturn(CompleteMultipartUploadResponse.builder().build());

    byte[] payload = new byte[2048];
    S3ObjectStorageClient client = newClient(s3Client, TEST_BUCKET, null, 1024, 1024);
    client.putObject("exports/large.bin", payload);

    verify(s3Client).createMultipartUpload(any(CreateMultipartUploadRequest.class));
    verify(s3Client, times(2)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
    verify(s3Client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    verify(s3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }

  @Test
  public void testPutObjectMultipartAbortsOnFailure() {
    S3Client s3Client = mock(S3Client.class);
    when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
    when(s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
        .thenThrow(new RuntimeException("upload failed"));

    S3ObjectStorageClient client = newClient(s3Client, TEST_BUCKET, null, 1024, 1024);
    try {
      client.putObject("exports/large.bin", new byte[2048]);
      throw new AssertionError("Expected RuntimeException");
    } catch (RuntimeException ignored) {
      verify(s3Client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testPutObjectRejectsUnconfiguredBucket() {
    S3ObjectStorageClient client = newClient(mock(S3Client.class), " ", null, 1024, 1024);
    client.putObject("key", new byte[] {1});
  }

  @Test
  public void testPresignedDownloadUrlSuccess() throws Exception {
    S3Presigner presigner = mock(S3Presigner.class);
    S3ObjectStorageClient client =
        newClient(mock(S3Client.class), presigner, TEST_BUCKET, null, 1024, 512);

    String expectedUrl = "https://test-bucket.s3.amazonaws.com/test-key?X-Amz-Signature=mocked";
    PresignedGetObjectRequest presignedRequest = mock(PresignedGetObjectRequest.class);
    when(presignedRequest.url()).thenReturn(new URL(expectedUrl));
    when(presigner.presignGetObject(any(GetObjectPresignRequest.class)))
        .thenReturn(presignedRequest);

    String actualUrl =
        client.presignedDownloadUrl(new ObjectStorageReference(TEST_BUCKET, "test-key"), 3600);

    assertEquals(actualUrl, expectedUrl);
    verify(presigner).presignGetObject(any(GetObjectPresignRequest.class));
  }

  @Test
  public void testPresignedDownloadUrlExceptionHandling() {
    S3Presigner presigner = mock(S3Presigner.class);
    S3ObjectStorageClient client =
        newClient(mock(S3Client.class), presigner, TEST_BUCKET, null, 1024, 512);

    when(presigner.presignGetObject(any(GetObjectPresignRequest.class)))
        .thenThrow(new RuntimeException("Presigner error"));

    assertThrows(
        RuntimeException.class,
        () ->
            client.presignedDownloadUrl(new ObjectStorageReference(TEST_BUCKET, "test-key"), 3600));
  }

  @Test
  public void testPresignedDownloadUrlRejectsMismatchedBucket() {
    S3ObjectStorageClient client =
        newClient(mock(S3Client.class), mock(S3Presigner.class), TEST_BUCKET, null, 1024, 512);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            client.presignedDownloadUrl(
                new ObjectStorageReference("other-bucket", "test-key"), 3600));
  }

  @Test
  public void testPresignedUploadUrlSuccess() throws Exception {
    S3Presigner presigner = mock(S3Presigner.class);
    S3ObjectStorageClient client =
        newClient(mock(S3Client.class), presigner, TEST_BUCKET, null, 1024, 512);

    String expectedUrl = "https://test-bucket.s3.amazonaws.com/test-key?X-Amz-Signature=mocked";
    PresignedPutObjectRequest presignedRequest = mock(PresignedPutObjectRequest.class);
    when(presignedRequest.url()).thenReturn(new URL(expectedUrl));
    when(presigner.presignPutObject(any(PutObjectPresignRequest.class)))
        .thenReturn(presignedRequest);

    String actualUrl =
        client.presignedUploadUrl(
            new ObjectStorageReference(TEST_BUCKET, "test-key"), 3600, "image/jpeg");

    assertNotNull(actualUrl);
    assertEquals(actualUrl, expectedUrl);
    verify(presigner).presignPutObject(any(PutObjectPresignRequest.class));
  }

  @Test
  public void testPresignedUploadUrlWithNullContentType() throws Exception {
    S3Presigner presigner = mock(S3Presigner.class);
    S3ObjectStorageClient client =
        newClient(mock(S3Client.class), presigner, TEST_BUCKET, null, 1024, 512);

    String expectedUrl = "https://test-bucket.s3.amazonaws.com/test-key?X-Amz-Signature=mocked";
    PresignedPutObjectRequest presignedRequest = mock(PresignedPutObjectRequest.class);
    when(presignedRequest.url()).thenReturn(new URL(expectedUrl));
    when(presigner.presignPutObject(any(PutObjectPresignRequest.class)))
        .thenReturn(presignedRequest);

    String actualUrl =
        client.presignedUploadUrl(new ObjectStorageReference(TEST_BUCKET, "test-key"), 3600, null);

    assertEquals(actualUrl, expectedUrl);
  }

  @Test
  public void testPresignedUploadUrlExceptionHandling() {
    S3Presigner presigner = mock(S3Presigner.class);
    S3ObjectStorageClient client =
        newClient(mock(S3Client.class), presigner, TEST_BUCKET, null, 1024, 512);

    when(presigner.presignPutObject(any(PutObjectPresignRequest.class)))
        .thenThrow(new RuntimeException("S3 presigner error"));

    assertThrows(
        RuntimeException.class,
        () ->
            client.presignedUploadUrl(
                new ObjectStorageReference(TEST_BUCKET, "test-key"), 3600, "image/jpeg"));
  }

  @Test
  public void testDeleteObjectSuccess() {
    S3Client s3Client = mock(S3Client.class);
    S3ObjectStorageClient client =
        newClient(s3Client, mock(S3Presigner.class), TEST_BUCKET, null, 1024, 512);

    DeleteObjectResponse deleteResponse = mock(DeleteObjectResponse.class);
    SdkHttpResponse httpResponse = mock(SdkHttpResponse.class);
    when(deleteResponse.sdkHttpResponse()).thenReturn(httpResponse);
    when(httpResponse.isSuccessful()).thenReturn(true);
    when(s3Client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(deleteResponse);

    client.deleteObject(new ObjectStorageReference(TEST_BUCKET, "test-key"));

    verify(s3Client).deleteObject(any(DeleteObjectRequest.class));
  }

  @Test
  public void testDeleteObjectFailure() {
    S3Client s3Client = mock(S3Client.class);
    S3ObjectStorageClient client =
        newClient(s3Client, mock(S3Presigner.class), TEST_BUCKET, null, 1024, 512);

    DeleteObjectResponse deleteResponse = mock(DeleteObjectResponse.class);
    SdkHttpResponse httpResponse = mock(SdkHttpResponse.class);
    when(deleteResponse.sdkHttpResponse()).thenReturn(httpResponse);
    when(httpResponse.isSuccessful()).thenReturn(false);
    when(httpResponse.statusCode()).thenReturn(403);
    when(s3Client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(deleteResponse);

    assertThrows(
        RuntimeException.class,
        () -> client.deleteObject(new ObjectStorageReference(TEST_BUCKET, "test-key")));
  }

  @Test
  public void testDeleteObjectException() {
    S3Client s3Client = mock(S3Client.class);
    S3ObjectStorageClient client =
        newClient(s3Client, mock(S3Presigner.class), TEST_BUCKET, null, 1024, 512);

    when(s3Client.deleteObject(any(DeleteObjectRequest.class)))
        .thenThrow(new RuntimeException("S3 service error"));

    assertThrows(
        RuntimeException.class,
        () -> client.deleteObject(new ObjectStorageReference(TEST_BUCKET, "test-key")));
  }

  @Test
  public void testDeleteObjectRejectsMismatchedBucket() {
    S3ObjectStorageClient client =
        newClient(mock(S3Client.class), mock(S3Presigner.class), TEST_BUCKET, null, 1024, 512);

    assertThrows(
        IllegalArgumentException.class,
        () -> client.deleteObject(new ObjectStorageReference("other-bucket", "test-key")));
  }
}
