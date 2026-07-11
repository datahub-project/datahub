package com.linkedin.metadata.utils.objectstorage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class S3ObjectStorageClientTest {

  @Test
  public void testPutObjectAtBucketRootWhenNoPrefix() {
    S3Client s3Client = mock(S3Client.class);
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().build());

    S3ObjectStorageClient client =
        new S3ObjectStorageClient(s3Client, "my-bucket", null, 1024, 512);
    client.putObject("exports/out.bin", new byte[] {1, 2, 3});

    ArgumentCaptor<PutObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));
    assertEquals(requestCaptor.getValue().bucket(), "my-bucket");
    assertEquals(requestCaptor.getValue().key(), "exports/out.bin");
  }

  @Test
  public void testPutObjectSinglePart() {
    S3Client s3Client = mock(S3Client.class);
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().build());

    S3ObjectStorageClient client =
        new S3ObjectStorageClient(s3Client, "my-bucket", "prefix", 1024, 512);
    client.putObject("exports/out.bin", new byte[] {1, 2, 3});

    ArgumentCaptor<PutObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));
    assertEquals(requestCaptor.getValue().bucket(), "my-bucket");
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
    S3ObjectStorageClient client =
        new S3ObjectStorageClient(s3Client, "my-bucket", null, 1024, 1024);
    client.putObject("exports/large.bin", payload);

    verify(s3Client).createMultipartUpload(any(CreateMultipartUploadRequest.class));
    verify(s3Client, times(2)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
    verify(s3Client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    verify(s3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }
}
