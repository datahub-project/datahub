package com.linkedin.datahub.graphql.util;

import com.linkedin.entity.client.EntityClient;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

@Slf4j
public class S3Util {

    private volatile S3Client s3Client;
    private final EntityClient entityClient;

    // For credential refresh when using assumed roles
    @Nullable private final StsClient stsClient;
    @Nullable private final String roleArn;
    private volatile Instant credentialsExpiry;

    // Thread-safe access to S3Client refresh
    private final ReentrantReadWriteLock clientLock = new ReentrantReadWriteLock();

    public S3Util(@Nonnull S3Client s3Client, @Nonnull EntityClient entityClient) {
        this.s3Client = s3Client;
        this.entityClient = entityClient;
        this.stsClient = null;
        this.roleArn = null;
        this.credentialsExpiry = null;
    }

    public S3Util(
            @Nonnull EntityClient entityClient, @Nonnull StsClient stsClient, @Nonnull String roleArn) {
        this.entityClient = entityClient;
        this.stsClient = stsClient;
        this.roleArn = roleArn;

        // Initialize with fresh credentials
        refreshS3ClientIfNeeded();
    }

    /**
     * Thread-safe method to refresh S3Client if credentials are expired or missing. Uses
     * double-checked locking pattern for performance.
     */
    private void refreshS3ClientIfNeeded() {
        // Fast path: check if refresh is needed without acquiring write lock
        if (s3Client != null
                && credentialsExpiry != null
                && Instant.now().isBefore(credentialsExpiry.minus(Duration.ofMinutes(5)))) {
            return; // Credentials are still valid (with 5-minute buffer)
        }

        clientLock.writeLock().lock();
        try {
            // Double-check after acquiring write lock
            if (s3Client != null
                    && credentialsExpiry != null
                    && Instant.now().isBefore(credentialsExpiry.minus(Duration.ofMinutes(5)))) {
                return;
            }

            log.info("Refreshing S3 client credentials for role: {}", roleArn);

            // Assume the role to get temporary credentials
            Credentials assumedCredentials =
                    assumeRole(stsClient, roleArn, "s3-streaming-upload-session");

            // Close old client if it exists
            if (s3Client != null) {
                try {
                    s3Client.close();
                } catch (Exception e) {
                    log.warn("Failed to close old S3 client", e);
                }
            }

            // Update client and expiry time atomically
            this.s3Client = createS3ClientWithAssumedRole(stsClient, roleArn);
            this.credentialsExpiry = assumedCredentials.expiration();

            log.info(
                    "Successfully refreshed S3 client. New credentials expire at: {}", credentialsExpiry);

        } catch (Exception e) {
            log.error("Failed to refresh S3 client with assumed role: roleArn={}", roleArn, e);
            throw new RuntimeException(
                    "Failed to refresh S3 client with assumed role: " + e.getMessage(), e);
        } finally {
            clientLock.writeLock().unlock();
        }
    }

    /** This is not thread safe and should not be used directly */
    private static S3Client createS3ClientWithAssumedRole(
            @Nonnull StsClient stsClient, @Nonnull String roleArn) {
        try {
            // Assume the role to get temporary credentials
            Credentials assumedCredentials = assumeRole(stsClient, roleArn, "s3-session");

            // Create AWS credentials from the assumed role credentials
            AwsCredentials awsCredentials =
                    AwsSessionCredentials.create(
                            assumedCredentials.accessKeyId(),
                            assumedCredentials.secretAccessKey(),
                            assumedCredentials.sessionToken());

            var clientBuilder =
                    S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(awsCredentials));

            // Configure endpoint URL if provided (for LocalStack or custom S3 endpoints)
            String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
            if (endpointUrl != null && !endpointUrl.isEmpty()) {
                clientBuilder.endpointOverride(java.net.URI.create(endpointUrl));
                // Force path-style access for LocalStack compatibility
                clientBuilder.forcePathStyle(true);
            }

            return clientBuilder.build();

        } catch (Exception e) {
            log.error("Failed to create S3 client with assumed role: roleArn={}", roleArn, e);
            throw new RuntimeException(
                    "Failed to create S3 client with assumed role: " + e.getMessage(), e);
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
        // Refresh credentials if needed (only for assumed role scenarios)
        if (stsClient != null && roleArn != null) {
            refreshS3ClientIfNeeded();
        }

        clientLock.readLock().lock();
        try {
            // Create a pre-signer using the same configuration as the S3 client
            try (S3Presigner presigner =
                         S3Presigner.builder()
                                 .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
                                 .region(s3Client.serviceClientConfiguration().region())
                                 .build()) {

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
        } finally {
            clientLock.readLock().unlock();
        }
    }

    /**
     * Generate a pre-signed URL for uploading an S3 object
     *
     * @param bucket The S3 bucket name
     * @param key The S3 object key
     * @param expirationSeconds The expiration time in seconds
     * @param contentType The content type of the object to be uploaded (e.g., "image/jpeg", "application/pdf")
     * @return The pre-signed URL
     */
    public String generatePresignedUploadUrl(
            @Nonnull String bucket, @Nonnull String key, int expirationSeconds, @Nullable String contentType) {
        // Refresh credentials if needed (only for assumed role scenarios)
        if (stsClient != null && roleArn != null) {
            refreshS3ClientIfNeeded();
        }

        clientLock.readLock().lock();
        try {
            // Create a pre-signer using the same configuration as the S3 client
            try (S3Presigner presigner =
                         S3Presigner.builder()
                                 .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
                                 .region(s3Client.serviceClientConfiguration().region())
                                 .build()) {

                // Create the PutObjectRequest
                PutObjectRequest putObjectRequest =
                        PutObjectRequest.builder().bucket(bucket).key(key).contentType(contentType).build();

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
        } finally {
            clientLock.readLock().unlock();
        }
    }
}
