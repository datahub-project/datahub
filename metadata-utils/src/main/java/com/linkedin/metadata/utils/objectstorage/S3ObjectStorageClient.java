package com.linkedin.metadata.utils.objectstorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

@Slf4j
public class S3ObjectStorageClient implements ObjectStorageClient {

  public static final int DEFAULT_MULTIPART_THRESHOLD_BYTES = 8 * 1024 * 1024;
  public static final int DEFAULT_MULTIPART_PART_SIZE_BYTES = 8 * 1024 * 1024;

  @Nonnull private final S3Client s3Client;
  @Nonnull private final S3Presigner s3Presigner;
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
    this(
        s3Client,
        createPresigner(s3Client),
        bucketName,
        pathPrefix,
        multipartThresholdBytes,
        multipartPartSizeBytes);
  }

  public S3ObjectStorageClient(
      @Nonnull S3Client s3Client,
      @Nonnull S3Presigner s3Presigner,
      @Nonnull String bucketName,
      @Nullable String pathPrefix,
      int multipartThresholdBytes,
      int multipartPartSizeBytes) {
    this.s3Client = s3Client;
    this.s3Presigner = s3Presigner;
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

  @Override
  public boolean supportsPresignedUrls() {
    return true;
  }

  @Override
  @Nonnull
  public String storageBucket() {
    return bucketName;
  }

  @Override
  @Nonnull
  public String presignedDownloadUrl(@Nonnull ObjectStorageReference ref, int expirationSeconds) {
    validateReference(ref);
    try {
      GetObjectRequest getObjectRequest =
          GetObjectRequest.builder().bucket(ref.bucket()).key(ref.key()).build();
      GetObjectPresignRequest presignRequest =
          GetObjectPresignRequest.builder()
              .signatureDuration(Duration.ofSeconds(expirationSeconds))
              .getObjectRequest(getObjectRequest)
              .build();
      return s3Presigner.presignGetObject(presignRequest).url().toString();
    } catch (RuntimeException e) {
      log.error("Failed to generate presigned download URL for {}", ref, e);
      throw new RuntimeException("Failed to generate presigned download URL: " + e.getMessage(), e);
    }
  }

  @Override
  @Nonnull
  public String presignedUploadUrl(
      @Nonnull ObjectStorageReference ref, int expirationSeconds, @Nullable String contentType) {
    validateReference(ref);
    try {
      PutObjectRequest.Builder putObjectRequestBuilder =
          PutObjectRequest.builder().bucket(ref.bucket()).key(ref.key());
      if (contentType != null) {
        putObjectRequestBuilder.contentType(contentType);
      }
      PutObjectPresignRequest presignRequest =
          PutObjectPresignRequest.builder()
              .signatureDuration(Duration.ofSeconds(expirationSeconds))
              .putObjectRequest(putObjectRequestBuilder.build())
              .build();
      return s3Presigner.presignPutObject(presignRequest).url().toString();
    } catch (RuntimeException e) {
      log.error("Failed to generate presigned upload URL for {}", ref, e);
      throw new RuntimeException("Failed to generate presigned upload URL: " + e.getMessage(), e);
    }
  }

  @Override
  public void deleteObject(@Nonnull ObjectStorageReference ref) {
    validateReference(ref);
    try {
      DeleteObjectRequest deleteObjectRequest =
          DeleteObjectRequest.builder().bucket(ref.bucket()).key(ref.key()).build();
      DeleteObjectResponse response = s3Client.deleteObject(deleteObjectRequest);
      if (!response.sdkHttpResponse().isSuccessful()) {
        throw new RuntimeException(
            "Failed to delete object from S3. Response: "
                + response.sdkHttpResponse().statusCode());
      }
      log.info("Deleted object from s3://{}/{}", ref.bucket(), ref.key());
    } catch (RuntimeException e) {
      log.error("Failed to delete object from S3: {}", ref, e);
      throw new RuntimeException("Failed to delete object from S3: " + e.getMessage(), e);
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

  private void validateReference(@Nonnull ObjectStorageReference ref) {
    if (!bucketName.equals(ref.bucket())) {
      throw new IllegalArgumentException(
          "Object storage reference bucket does not match configured bucket");
    }
  }

  @Nonnull
  private static S3Presigner createPresigner(@Nonnull S3Client s3Client) {
    var presignerBuilder =
        S3Presigner.builder()
            .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
            .region(s3Client.serviceClientConfiguration().region());

    String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
    if (endpointUrl == null || endpointUrl.isEmpty()) {
      endpointUrl = System.getProperty("AWS_ENDPOINT_URL");
    }
    if (endpointUrl != null && !endpointUrl.isEmpty()) {
      presignerBuilder.endpointOverride(java.net.URI.create(endpointUrl));
      presignerBuilder.serviceConfiguration(
          S3Configuration.builder().pathStyleAccessEnabled(true).build());
    }

    return presignerBuilder.build();
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
