package com.linkedin.metadata.utils.aws;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Copies credentials off an existing AWS SDK client. An empty/missing provider must not be passed
 * to {@code S3Presigner.builder()} — SDK 2.30 then builds a new IRSA default chain ({@code
 * StsAssumeRoleWithWebIdentityCredentialsProvider} per call).
 */
public final class AwsClientCredentials {

  private AwsClientCredentials() {}

  @Nonnull
  public static AwsCredentialsProvider requireFrom(@Nonnull S3Client s3Client) {
    return require(s3Client.serviceClientConfiguration().credentialsProvider());
  }

  @Nonnull
  static AwsCredentialsProvider require(@Nullable Object rawCredentialsProvider) {
    AwsCredentialsProvider provider = unwrap(rawCredentialsProvider);
    if (provider == null) {
      throw new IllegalStateException(
          "S3Client has no credentialsProvider; refusing implicit AWS default credential chain");
    }
    return provider;
  }

  @Nullable
  static AwsCredentialsProvider unwrap(@Nullable Object rawCredentialsProvider) {
    if (rawCredentialsProvider == null) {
      return null;
    }
    if (rawCredentialsProvider instanceof Optional<?> optional) {
      return unwrap(optional.orElse(null));
    }
    if (rawCredentialsProvider instanceof AwsCredentialsProvider awsCredentialsProvider) {
      return awsCredentialsProvider;
    }
    return null;
  }
}
