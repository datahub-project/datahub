package com.linkedin.gms.factory.kafka;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.msk.auth.iam.IAMClientCallbackHandler;
import software.amazon.msk.auth.iam.IAMLoginModule;
import software.amazon.msk.auth.iam.internals.AWSCredentialsCallback;

/**
 * Reuses the process-owned credentials provider for MSK IAM authentication.
 *
 * <p>{@code aws-msk-iam-auth} creates an async web-identity provider for every Kafka callback
 * handler but does not close that fallback provider. The scheduled refresh task then retains an STS
 * and HTTP client after Kafka discards the handler.
 */
public final class DataHubMskIamClientCallbackHandler extends IAMClientCallbackHandler {

  @Nullable private static volatile AwsCredentialsProvider sharedCredentialsProvider;

  public static void installSharedCredentials(@Nullable AwsCredentialsProvider provider) {
    if (provider != null) {
      sharedCredentialsProvider = provider;
    }
  }

  public static void resetIfInstalled(@Nullable AwsCredentialsProvider provider) {
    if (provider != null && provider == sharedCredentialsProvider) {
      sharedCredentialsProvider = null;
    }
  }

  static boolean isSharedCredentialsInstalled() {
    return sharedCredentialsProvider != null;
  }

  @Override
  public void configure(
      Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
    if (!IAMLoginModule.MECHANISM.equals(saslMechanism)) {
      throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism);
    }
    if (sharedCredentialsProvider == null) {
      throw new IllegalStateException("Shared AWS credentials are unavailable for MSK IAM");
    }
  }

  @Override
  protected void handleCallback(AWSCredentialsCallback callback) {
    AwsCredentialsProvider provider = sharedCredentialsProvider;
    if (provider == null) {
      callback.setLoadingException(
          new IllegalStateException("Shared AWS credentials are unavailable for MSK IAM"));
      return;
    }
    try {
      callback.setAwsCredentials(provider.resolveCredentials());
    } catch (RuntimeException e) {
      callback.setLoadingException(e);
    }
  }

  @Override
  public void close() {
    // AwsClientFactory owns and closes the process-wide provider.
  }
}
