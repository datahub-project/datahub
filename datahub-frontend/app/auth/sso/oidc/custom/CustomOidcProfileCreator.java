package auth.sso.oidc.custom;

import auth.sso.oidc.OidcConfigs;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import java.io.IOException;
import java.net.URI;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.exceptions.UserInfoErrorResponseException;
import org.pac4j.oidc.profile.creator.OidcProfileCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom OIDC Profile Creator that adds retry logic to the userinfo endpoint call. This addresses
 * the "Invalid Http response" errors that can occur when the userinfo endpoint is temporarily
 * unavailable or slow to respond.
 */
public class CustomOidcProfileCreator extends OidcProfileCreator {

  private static final Logger logger = LoggerFactory.getLogger(CustomOidcProfileCreator.class);
  private final OidcConfigs oidcConfigs;

  public CustomOidcProfileCreator(
      OidcConfiguration configuration, OidcClient client, OidcConfigs oidcConfigs) {
    super(configuration, client);
    this.oidcConfigs = oidcConfigs;
  }

  @Override
  public void callUserInfoEndpoint(
      URI userInfoEndpointUri, AccessToken accessToken, UserProfile profile)
      throws IOException, ParseException, java.text.ParseException, UserInfoErrorResponseException {
    // Wrap the parent's callUserInfoEndpoint method with retry logic
    callUserInfoEndpointWithRetry(userInfoEndpointUri, accessToken, profile);
  }

  /**
   * Calls the userinfo endpoint with retry logic and exponential backoff. This wraps the parent's
   * callUserInfoEndpoint method with retry logic.
   */
  private void callUserInfoEndpointWithRetry(
      URI userInfoEndpointUri, AccessToken accessToken, UserProfile profile)
      throws IOException, ParseException, java.text.ParseException, UserInfoErrorResponseException {
    int maxAttempts;
    try {
      maxAttempts = Integer.parseInt(oidcConfigs.getHttpRetryAttempts());
    } catch (NumberFormatException e) {
      logger.warn(
          "Invalid retry attempts configuration: {}, defaulting to 1",
          oidcConfigs.getHttpRetryAttempts());
      maxAttempts = 1;
    }

    long initialDelay;
    try {
      initialDelay = Long.parseLong(oidcConfigs.getHttpRetryDelay());
    } catch (NumberFormatException e) {
      logger.warn(
          "Invalid retry delay configuration: {}, defaulting to 1000ms",
          oidcConfigs.getHttpRetryDelay());
      initialDelay = 1000;
    }

    // Ensure at least one attempt
    if (maxAttempts <= 0) {
      maxAttempts = 1;
    }

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        // Call the parent's method directly
        super.callUserInfoEndpoint(userInfoEndpointUri, accessToken, profile);
        return; // Success, exit the retry loop
      } catch (IOException | ParseException | UserInfoErrorResponseException e) {
        if (attempt == maxAttempts) {
          throw e; // Rethrow on final attempt
        }
        try {
          // Exponential backoff
          Thread.sleep(initialDelay * (long) Math.pow(2, attempt - 1));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("UserInfo request retry interrupted", ie);
        }
        logger.warn("UserInfo request retry attempt {} of {} failed", attempt, maxAttempts, e);
      }
    }
  }
}
