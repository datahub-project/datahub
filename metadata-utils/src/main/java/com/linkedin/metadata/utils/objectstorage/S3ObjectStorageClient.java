package com.linkedin.metadata.utils.objectstorage;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@Slf4j
public class S3ObjectStorageClient implements ObjectStorageClient {

  public static final int DEFAULT_MULTIPART_THRESHOLD_BYTES = 8 * 1024 * 1024;
  public static final int DEFAULT_MULTIPART_PART_SIZE_BYTES = 8 * 1024 * 1024;

  @Nonnull private final S3Client s3Client;
  @Nonnull private final String bucketName;
  @Nullable private final String pathPrefix;
  private final int multipartThresholdBytes;
  private final int multipartPartSizeBytes;

  public S3ObjectStorageClient(
      @Nonnull S3Client s3Client, @Nonnull String bucketName, @Nullable String pathPrefix) {
    this(
        s3Client,
        bucketName,
        pathPrefix,
        DEFAULT_MULTIPART_THRESHOLD_BYTES,
        DEFAULT_MULTIPART_PART_SIZE_BYTES);
  }

  public S3ObjectStorageClient(
      @Nonnull S3Client s3Client,
      @Nonnull String bucketName,
      @Nullable String pathPrefix,
      int multipartThresholdBytes,
      int multipartPartSizeBytes) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.pathPrefix = pathPrefix;
    this.multipartThresholdBytes = multipartThresholdBytes;
    this.multipartPartSizeBytes = multipartPartSizeBytes;
  }

  @Override
  public void putObject(@Nonnull String objectKey, @Nonnull byte[] bytes) {
    if (!isConfigured()) {
      throw new IllegalStateException("S3 bucket name is not configured");
    }
    String key = ObjectStorageKeyResolver.joinKey(pathPrefix, objectKey, ObjectStorageProvider.S3);
    if (bytes.length < multipartThresholdBytes) {
      putObjectSinglePart(key, bytes);
    } else {
      putObjectMultipart(key, bytes);
    }
  }

  private void putObjectSinglePart(@Nonnull String key, @Nonnull byte[] bytes) {
    s3Client.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(key).build(),
        RequestBody.fromBytes(bytes));
    log.debug("Uploaded {} bytes to s3://{}/{}", bytes.length, bucketName, key);
  }

  private void putObjectMultipart(@Nonnull String key, @Nonnull byte[] bytes) {
    CreateMultipartUploadRequest createRequest =
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(key).build();
    String uploadId = s3Client.createMultipartUpload(createRequest).uploadId();

    List<CompletedPart> completedParts = new ArrayList<>();
    try {
      int partNumber = 1;
      for (int offset = 0; offset < bytes.length; offset += multipartPartSizeBytes) {
        int length = Math.min(multipartPartSizeBytes, bytes.length - offset);
        byte[] partBytes = new byte[length];
        System.arraycopy(bytes, offset, partBytes, 0, length);

        UploadPartRequest uploadPartRequest =
            UploadPartRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();
        String eTag =
            s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(partBytes)).eTag();
        completedParts.add(CompletedPart.builder().partNumber(partNumber).eTag(eTag).build());
        partNumber++;
      }

      s3Client.completeMultipartUpload(
          CompleteMultipartUploadRequest.builder()
              .bucket(bucketName)
              .key(key)
              .uploadId(uploadId)
              .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
              .build());
      log.debug(
          "Multipart uploaded {} bytes ({} parts) to s3://{}/{}",
          bytes.length,
          completedParts.size(),
          bucketName,
          key);
    } catch (RuntimeException e) {
      s3Client.abortMultipartUpload(
          AbortMultipartUploadRequest.builder()
              .bucket(bucketName)
              .key(key)
              .uploadId(uploadId)
              .build());
      throw e;
    }
  }

  @Override
  @Nonnull
  public ObjectStorageProvider provider() {
    return ObjectStorageProvider.S3;
  }

  @Override
  public boolean isConfigured() {
    return bucketName != null && !bucketName.isBlank();
  }
}
