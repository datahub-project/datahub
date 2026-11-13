package com.datahub.authentication;

import java.util.List;
import lombok.Data;

/** POJO representing the "authentication" configuration block in application.yaml. */
@Data
public class AuthenticationConfiguration {
  /** Whether authentication is enabled */
  private boolean enabled;

  /** Whether user existence is enforced */
  private boolean enforceExistenceEnabled;

  /**
   * When true, login-denial handling emits an additional INFO line using the same {@code
   * loginDenied} message shape as the default line, with the raw (unmasked) {@code userRef}
   * (sensitive). Binds to {@code AUTH_VERBOSE_LOGGING}.
   */
  private boolean verboseAuthFailureLogging;

  /** Paths to be excluded from filtering * */
  private String excludedPaths;

  /**
   * List of configurations for {@link com.datahub.plugins.auth.authentication.Authenticator}s to be
   * registered
   */
  private List<AuthenticatorConfiguration> authenticators;

  /** Unique id to identify internal system callers */
  private String systemClientId;

  /** Unique secret to authenticate internal system callers */
  private String systemClientSecret;

  /** The lifespan of a UI session token. */
  private long sessionTokenDurationMs;

  /** The lifespan of a password reset token in milliseconds. Defaults to 24 hours. */
  private long passwordResetTokenExpirationMs;

  private TokenServiceConfiguration tokenService;

  /** Policy for personal / service-account access token durations. */
  private AccessTokenConfiguration accessTokens = AccessTokenConfiguration.defaults();

  /**
   * Retry-After, in seconds, on the 503 returned when credentials cannot be checked because the
   * token store is unavailable. Binds to {@code AUTH_UNAVAILABLE_RETRY_AFTER_SECONDS}.
   */
  private int unavailableRetryAfterSeconds = 3;

  /**
   * Random extra delay added to {@link #unavailableRetryAfterSeconds}, as a percentage of it
   * (0-100), so clients that failed together do not all retry at the same moment. Binds to {@code
   * AUTH_UNAVAILABLE_RETRY_AFTER_JITTER_PERCENT}.
   */
  private int unavailableRetryAfterJitterPercent = 50;
}
