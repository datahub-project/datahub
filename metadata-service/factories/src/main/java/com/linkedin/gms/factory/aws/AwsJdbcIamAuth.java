package com.linkedin.gms.factory.aws;

import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.jdbc.authentication.AwsCredentialsManager;

/**
 * The AWS JDBC wrapper's IAM plugin calls {@code DefaultCredentialsProvider.builder().build()} on
 * every token mint unless a process-wide {@link AwsCredentialsManager} handler is set. Bind that
 * handler to the GMS-owned provider so IRSA refresh tasks are not created per reconnect.
 */
@Slf4j
public final class AwsJdbcIamAuth {

  @Nullable private static volatile AwsCredentialsProvider installed;

  private AwsJdbcIamAuth() {}

  public static void installSharedCredentials(@Nullable AwsCredentialsProvider shared) {
    if (shared == null) {
      return;
    }
    installed = shared;
    AwsCredentialsManager.setCustomHandler((hostSpec, properties) -> shared);
    log.info("Bound AWS JDBC IAM credentials to the process-wide DefaultCredentialsProvider");
  }

  public static void reset() {
    AwsCredentialsManager.resetCustomHandler();
    installed = null;
  }

  /**
   * Clears the JDBC handler only if {@code provider} is the one this class currently bound. Other
   * Spring contexts / factories that did not install credentials must not wipe a live handler.
   */
  public static void resetIfInstalled(@Nullable AwsCredentialsProvider provider) {
    if (provider == null || provider != installed) {
      return;
    }
    reset();
  }
}
