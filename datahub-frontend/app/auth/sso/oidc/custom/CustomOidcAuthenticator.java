package auth.sso.oidc.custom;

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.ClientSecretPost;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.pkce.CodeVerifier;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponseParser;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import com.nimbusds.openid.connect.sdk.token.OIDCTokens;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.credentials.Credentials;
import org.pac4j.core.exception.TechnicalException;
import org.pac4j.core.util.CommonHelper;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.pac4j.oidc.credentials.authenticator.OidcAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomOidcAuthenticator extends OidcAuthenticator {

  private static final Logger logger = LoggerFactory.getLogger(CustomOidcAuthenticator.class);

  private static final Collection<ClientAuthenticationMethod> SUPPORTED_METHODS =
      Arrays.asList(
          ClientAuthenticationMethod.CLIENT_SECRET_POST,
          ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
          ClientAuthenticationMethod.NONE);

  private final ClientAuthentication clientAuthentication;

  public CustomOidcAuthenticator(final OidcClient client) {
    super(client.getConfiguration(), client);

    // check authentication methods
    OIDCProviderMetadata providerMetadata;
    try {
      providerMetadata = loadWithRetry();
    } catch (TechnicalException e) {
      logger.error(
          "Could not resolve identity provider's remote configuration from DiscoveryURI: {}",
          configuration.getDiscoveryURI());
      throw e;
    }

    List<ClientAuthenticationMethod> metadataMethods =
        providerMetadata.getTokenEndpointAuthMethods();

    final ClientAuthenticationMethod preferredMethod =
        getPreferredAuthenticationMethod(configuration);

    final ClientAuthenticationMethod chosenMethod;
    if (CommonHelper.isNotEmpty(metadataMethods)) {
      if (preferredMethod != null) {
        if (ClientAuthenticationMethod.NONE.equals(preferredMethod)
            || metadataMethods.contains(preferredMethod)) {
          chosenMethod = preferredMethod;
        } else {
          throw new TechnicalException(
              "Preferred authentication method ("
                  + preferredMethod
                  + ") not supported "
                  + "by provider according to provider metadata ("
                  + metadataMethods
                  + ").");
        }
      } else {
        chosenMethod = firstSupportedMethod(metadataMethods);
      }
    } else {
      chosenMethod =
          preferredMethod != null ? preferredMethod : ClientAuthenticationMethod.getDefault();
      logger.info(
          "Provider metadata does not provide Token endpoint authentication methods. Using: {}",
          chosenMethod);
    }

    final ClientID clientID = new ClientID(configuration.getClientId());
    if (ClientAuthenticationMethod.CLIENT_SECRET_POST.equals(chosenMethod)) {
      final Secret secret = new Secret(configuration.getSecret());
      clientAuthentication = new ClientSecretPost(clientID, secret);
    } else if (ClientAuthenticationMethod.CLIENT_SECRET_BASIC.equals(chosenMethod)) {
      final Secret secret = new Secret(configuration.getSecret());
      clientAuthentication = new ClientSecretBasic(clientID, secret);
    } else if (ClientAuthenticationMethod.NONE.equals(chosenMethod)) {
      clientAuthentication = null; // No client authentication in none mode
    } else {
      throw new TechnicalException("Unsupported client authentication method: " + chosenMethod);
    }
  }

  /**
   * The preferred {@link ClientAuthenticationMethod} specified in the given {@link
   * OidcConfiguration}, or <code>null</code> meaning that the a provider-supported method should be
   * chosen.
   */
  private static ClientAuthenticationMethod getPreferredAuthenticationMethod(
      OidcConfiguration config) {
    final ClientAuthenticationMethod configurationMethod = config.getClientAuthenticationMethod();
    if (configurationMethod == null) {
      return null;
    }

    if (!SUPPORTED_METHODS.contains(configurationMethod)) {
      throw new TechnicalException(
          "Configured authentication method (" + configurationMethod + ") is not supported.");
    }

    return configurationMethod;
  }

  /**
   * The first {@link ClientAuthenticationMethod} from the given list of methods that is supported
   * by this implementation.
   *
   * @throws TechnicalException if none of the provider-supported methods is supported.
   */
  private static ClientAuthenticationMethod firstSupportedMethod(
      final List<ClientAuthenticationMethod> metadataMethods) {
    Optional<ClientAuthenticationMethod> firstSupported =
        metadataMethods.stream().filter((m) -> SUPPORTED_METHODS.contains(m)).findFirst();
    if (firstSupported.isPresent()) {
      return firstSupported.get();
    } else {
      throw new TechnicalException(
          "None of the Token endpoint provider metadata authentication methods are supported: "
              + metadataMethods);
    }
  }

  @Override
  public Optional<Credentials> validate(CallContext ctx, Credentials cred) {
    OidcCredentials credentials = (OidcCredentials) cred;
    WebContext context = ctx.webContext();

    final AuthorizationCode code = credentials.toAuthorizationCode();
    // if we have a code
    if (code != null) {
      try {
        final String computedCallbackUrl = client.computeFinalCallbackUrl(context);
        CodeVerifier verifier =
            (CodeVerifier)
                configuration
                    .getValueRetriever()
                    .retrieve(ctx, client.getCodeVerifierSessionAttributeName(), client)
                    .orElse(null);
        // Token request
        final TokenRequest request =
            createTokenRequest(
                new AuthorizationCodeGrant(code, new URI(computedCallbackUrl), verifier));
        HTTPRequest tokenHttpRequest = request.toHTTPRequest();
        tokenHttpRequest.setConnectTimeout(configuration.getConnectTimeout());
        tokenHttpRequest.setReadTimeout(configuration.getReadTimeout());

        final HTTPResponse httpResponse = tokenHttpRequest.send();
        logger.debug(
            "Token response: status={}, content={}",
            httpResponse.getStatusCode(),
            httpResponse.getContent());

        final TokenResponse response = OIDCTokenResponseParser.parse(httpResponse);
        if (response instanceof TokenErrorResponse) {
          throw new TechnicalException(
              "Bad token response, error=" + ((TokenErrorResponse) response).getErrorObject());
        }
        logger.debug("Token response successful");
        final OIDCTokenResponse tokenSuccessResponse = (OIDCTokenResponse) response;

        // save tokens in credentials
        final OIDCTokens oidcTokens = tokenSuccessResponse.getOIDCTokens();
        credentials.setAccessTokenObject(oidcTokens.getAccessToken());

        // Only set refresh token if it exists
        if (oidcTokens.getRefreshToken() != null) {
          credentials.setRefreshTokenObject(oidcTokens.getRefreshToken());
        }

        if (oidcTokens.getIDToken() != null) {
          credentials.setIdToken(oidcTokens.getIDToken().getParsedString());
        }

      } catch (final URISyntaxException | IOException | ParseException e) {
        throw new TechnicalException(e);
      }
    }

    return Optional.ofNullable(cred);
  }

  // Simple retry with exponential backoff
  public OIDCProviderMetadata loadWithRetry() {
    int maxAttempts = 3;
    long initialDelay = 1000; // 1 second

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        OIDCProviderMetadata providerMetadata = configuration.getOpMetadataResolver().load();
        return Objects.requireNonNull(providerMetadata);
      } catch (RuntimeException e) {
        if (attempt == maxAttempts) {
          throw e; // Rethrow on final attempt
        }
        try {
          // Exponential backoff
          Thread.sleep(initialDelay * (long) Math.pow(2, attempt - 1));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Retry interrupted", ie);
        }
        logger.warn("Retry attempt {} of {} failed", attempt, maxAttempts, e);
      }
    }
    throw new RuntimeException(
        "Failed to load provider metadata after " + maxAttempts + " attempts");
  }
}
