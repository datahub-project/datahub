package com.linkedin.metadata.utils.aws;

import java.time.Duration;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

@Slf4j
public class S3Util {

  private final S3Client s3Client;

  // Cached S3Presigner instance (or injected for testing purposes)
  @Nonnull private final S3Presigner s3Presigner;

  public S3Util(@Nonnull S3Client s3Client) {
    this(s3Client, null);
  }

  public S3Util(@Nonnull S3Client s3Client, @Nullable S3Presigner s3Presigner) {
    this.s3Client = s3Client;
    this.s3Presigner = s3Presigner != null ? s3Presigner : createPresigner();
  }

  public S3Util(@Nonnull StsClient stsClient, @Nonnull String roleArn) {
    this(stsClient, roleArn, null);
  }

  public S3Util(
      @Nonnull StsClient stsClient, @Nonnull String roleArn, @Nullable S3Presigner s3Presigner) {

    this.s3Client = createS3Client(stsClient, roleArn);
    this.s3Presigner = s3Presigner != null ? s3Presigner : createPresigner();
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
        log.info("Configuring S3Client with custom endpoint: {}", endpointUrl);
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

  public static Credentials assumeRole(
      @Nonnull StsClient stsClient, @Nonnull String roleArn, @Nonnull String sessionName) {
    try {
      AssumeRoleRequest roleRequest =
          AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName(sessionName).build();

      AssumeRoleResponse roleResponse = stsClient.assumeRole(roleRequest);
      return roleResponse.credentials();

    } catch (Exception e) {
      log.error("Failed to assume role: roleArn={}, sessionName={}", roleArn, sessionName, e);
      throw new RuntimeException("Failed to assume AWS role: " + e.getMessage(), e);
    }
  }

  private S3Presigner createPresigner() {
    // Create and cache the presigner to avoid creating/closing it repeatedly
    var presignerBuilder =
        S3Presigner.builder()
            .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
            .region(s3Client.serviceClientConfiguration().region());

    // Apply the same endpoint configuration as the S3Client
    String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
    if (endpointUrl != null && !endpointUrl.isEmpty()) {
      presignerBuilder.endpointOverride(java.net.URI.create(endpointUrl));
      presignerBuilder.serviceConfiguration(
          S3Configuration.builder().pathStyleAccessEnabled(true).build());
    }

    return presignerBuilder.build();
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
      PresignedGetObjectRequest presignedRequest =
          this.s3Presigner.presignGetObject(presignRequest);
      return presignedRequest.url().toString();
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
      PresignedPutObjectRequest presignedRequest =
          this.s3Presigner.presignPutObject(presignRequest);
      return presignedRequest.url().toString();
    } catch (Exception e) {
      log.error("Failed to generate presigned upload URL for bucket: {}, key: {}", bucket, key, e);
      throw new RuntimeException("Failed to generate presigned upload URL: " + e.getMessage(), e);
    }
  }

  /**
   * Delete an object from S3
   *
   * @param bucket The S3 bucket name
   * @param key The S3 object key to delete
   */
  public void deleteObject(@Nonnull String bucket, @Nonnull String key) {
    try {
      DeleteObjectRequest deleteObjectRequest =
          DeleteObjectRequest.builder().bucket(bucket).key(key).build();

      DeleteObjectResponse response = s3Client.deleteObject(deleteObjectRequest);

      if (!response.sdkHttpResponse().isSuccessful()) {
        log.error(
            "Failed to delete object from S3. Bucket: {}, Key: {}, Response: {}",
            bucket,
            key,
            response);
        throw new RuntimeException(
            "Failed to delete object from S3. Response: "
                + response.sdkHttpResponse().statusCode());
      }

      log.info("Successfully deleted object from S3. Bucket: {}, Key: {}", bucket, key);
    } catch (Exception e) {
      log.error("Failed to delete object from S3. Bucket: {}, Key: {}", bucket, key, e);
      throw new RuntimeException("Failed to delete object from S3: " + e.getMessage(), e);
    }
  }
}
