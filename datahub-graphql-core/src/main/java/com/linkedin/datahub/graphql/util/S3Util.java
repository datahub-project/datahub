package com.linkedin.datahub.graphql.util;

import com.linkedin.entity.client.EntityClient;
import java.time.Duration;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

@Slf4j
public class S3Util {

  private final S3Client s3Client;
  private final EntityClient entityClient;

  // Optional S3Presigner for testing purposes
  @Nullable private final S3Presigner s3Presigner;

  public S3Util(@Nonnull S3Client s3Client, @Nonnull EntityClient entityClient) {
    this(s3Client, entityClient, null);
  }

  public S3Util(
      @Nonnull S3Client s3Client,
      @Nonnull EntityClient entityClient,
      @Nullable S3Presigner s3Presigner) {
    this.s3Client = s3Client;
    this.entityClient = entityClient;
    this.s3Presigner = s3Presigner;
  }

  public S3Util(
      @Nonnull EntityClient entityClient, @Nonnull StsClient stsClient, @Nonnull String roleArn) {
    this(entityClient, stsClient, roleArn, null);
  }

  public S3Util(
      @Nonnull EntityClient entityClient,
      @Nonnull StsClient stsClient,
      @Nonnull String roleArn,
      @Nullable S3Presigner s3Presigner) {
    this.entityClient = entityClient;
    this.s3Presigner = s3Presigner;
    this.s3Client = createS3Client(stsClient, roleArn);
  }

  /** Creates S3Client with StsAssumeRoleCredentialsProvider for automatic credential refresh. */
  private static S3Client createS3Client(@Nonnull StsClient stsClient, @Nonnull String roleArn) {
    try {
      log.info("Creating S3Client for role: {}", roleArn);

      StsAssumeRoleCredentialsProvider credentialsProvider =
          StsAssumeRoleCredentialsProvider.builder()
              .stsClient(stsClient)
              .refreshRequest(r -> r.roleArn(roleArn).roleSessionName("s3-session"))
              .asyncCredentialUpdateEnabled(true) // Enable background credential refresh
              .build();

      var clientBuilder = S3Client.builder().credentialsProvider(credentialsProvider);

      // Configure endpoint URL if provided (for LocalStack or custom S3 endpoints)
      String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
      if (endpointUrl != null && !endpointUrl.isEmpty()) {
        clientBuilder.endpointOverride(java.net.URI.create(endpointUrl));
        // Force path-style access for LocalStack compatibility
        clientBuilder.forcePathStyle(true);
      }

      S3Client client = clientBuilder.build();
      log.info("Successfully created S3Client for role: {}", roleArn);
      return client;

    } catch (Exception e) {
      log.error("Failed to create S3 client: roleArn={}", roleArn, e);
      throw new RuntimeException("Failed to create S3 clien: " + e.getMessage(), e);
    }
  }

  private S3Presigner getPresigner() {
    if (this.s3Presigner != null) {
      return this.s3Presigner;
    }

    return S3Presigner.builder()
        .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
        .region(s3Client.serviceClientConfiguration().region())
        .build();
  }

  /**
   * Generate a pre-signed URL for downloading an S3 object
   *
   * @param bucket The S3 bucket name
   * @param key The S3 object key
   * @param expirationSeconds The expiration time in seconds
   * @return The pre-signed URL
   */
  public String generatePresignedDownloadUrl(
      @Nonnull String bucket, @Nonnull String key, int expirationSeconds) {
    try {
      // Create a pre-signer using the same configuration as the S3 client
      try (S3Presigner presigner = getPresigner()) {

        // Create the GetObjectRequest
        GetObjectRequest getObjectRequest =
            GetObjectRequest.builder().bucket(bucket).key(key).build();

        // Create the presign request
        GetObjectPresignRequest presignRequest =
            GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofSeconds(expirationSeconds))
                .getObjectRequest(getObjectRequest)
                .build();

        // Generate the presigned URL
        PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);
        return presignedRequest.url().toString();
      }
    } catch (Exception e) {
      log.error("Failed to generate presigned URL for bucket: {}, key: {}", bucket, key, e);
      throw new RuntimeException("Failed to generate presigned URL: " + e.getMessage(), e);
    }
  }

  /**
   * Generate a pre-signed URL for uploading an S3 object
   *
   * @param bucket The S3 bucket name
   * @param key The S3 object key
   * @param expirationSeconds The expiration time in seconds
   * @param contentType The content type of the object to be uploaded (e.g., "image/jpeg",
   *     "application/pdf")
   * @return The pre-signed URL
   */
  public String generatePresignedUploadUrl(
      @Nonnull String bucket,
      @Nonnull String key,
      int expirationSeconds,
      @Nullable String contentType) {
    try {
      // Create a pre-signer using the same configuration as the S3 client
      try (S3Presigner presigner = getPresigner()) {

        // Create the PutObjectRequest
        PutObjectRequest putObjectRequest =
            PutObjectRequest.builder().bucket(bucket).contentType(contentType).key(key).build();

        // Create the presign request
        PutObjectPresignRequest presignRequest =
            PutObjectPresignRequest.builder()
                .signatureDuration(Duration.ofSeconds(expirationSeconds))
                .putObjectRequest(putObjectRequest)
                .build();

        // Generate the presigned URL
        PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);
        return presignedRequest.url().toString();
      }
    } catch (Exception e) {
      log.error("Failed to generate presigned upload URL for bucket: {}, key: {}", bucket, key, e);
      throw new RuntimeException("Failed to generate presigned upload URL: " + e.getMessage(), e);
    }
  }
}
