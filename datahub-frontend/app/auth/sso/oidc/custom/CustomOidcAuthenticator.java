package auth.sso.oidc.custom;

import auth.sso.oidc.OidcConfigs;
import auth.sso.oidc.PrivateKeyJwtUtils;
import com.nimbusds.jose.JWSAlgorithm;
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
import com.nimbusds.oauth2.sdk.auth.PrivateKeyJWT;
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
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
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
          ClientAuthenticationMethod.PRIVATE_KEY_JWT,
          ClientAuthenticationMethod.NONE);

  private final ClientAuthentication clientAuthentication;
  private final OidcConfigs oidcConfigs;

  public CustomOidcAuthenticator(final OidcClient client, final OidcConfigs oidcConfigs) {
    super(client.getConfiguration(), client);
    this.oidcConfigs = oidcConfigs;

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
    } else if (ClientAuthenticationMethod.PRIVATE_KEY_JWT.equals(chosenMethod)) {
      clientAuthentication =
          createPrivateKeyJwtAuthentication(
              clientID, providerMetadata.getTokenEndpointURI(), oidcConfigs);
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

  /**
   * Creates a PrivateKeyJWT client authentication for certificate-based SSO.
   *
   * @param clientID The OIDC client ID
   * @param tokenEndpoint The token endpoint URI from provider metadata
   * @param oidcConfigs The OIDC configuration containing key file paths
   * @return A PrivateKeyJWT client authentication instance
   * @throws TechnicalException if key loading or JWT creation fails
   */
  private ClientAuthentication createPrivateKeyJwtAuthentication(
      ClientID clientID, URI tokenEndpoint, OidcConfigs oidcConfigs) {
    try {
      String privateKeyPath =
          oidcConfigs
              .getPrivateKeyFilePath()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "privateKeyFilePath is required for private_key_jwt authentication"));

      PrivateKey privateKey =
          PrivateKeyJwtUtils.loadPrivateKey(
              privateKeyPath, oidcConfigs.getPrivateKeyPassword().orElse(null));

      String publicKeyPath =
          oidcConfigs
              .getPublicKeyFilePath()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "publicKeyFilePath is required for private_key_jwt authentication"));

      X509Certificate certificate = PrivateKeyJwtUtils.loadCertificate(publicKeyPath);
      String keyId = PrivateKeyJwtUtils.computeThumbprint(certificate);

      JWSAlgorithm algorithm = JWSAlgorithm.parse(oidcConfigs.getPrivateKeyJwtAlgorithm());

      logger.info(
          "Creating PrivateKeyJWT authentication with algorithm: {}, keyId: {}", algorithm, keyId);

      return new PrivateKeyJWT(clientID, tokenEndpoint, algorithm, privateKey, keyId, null);
    } catch (Exception e) {
      throw new TechnicalException(
          "Failed to create PrivateKeyJWT client authentication: " + e.getMessage(), e);
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
        // Token request with retry
        final OIDCTokenResponse tokenSuccessResponse =
            executeTokenRequestWithRetry(code, computedCallbackUrl, verifier);

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

  // Retry logic for token request with exponential backoff
  private OIDCTokenResponse executeTokenRequestWithRetry(
      AuthorizationCode code, String computedCallbackUrl, CodeVerifier verifier)
      throws URISyntaxException, IOException, ParseException {
    int maxAttempts = Integer.parseInt(oidcConfigs.getHttpRetryAttempts());
    long initialDelay = Long.parseLong(oidcConfigs.getHttpRetryDelay());

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
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
        return (OIDCTokenResponse) response;

      } catch (IOException | ParseException | TechnicalException e) {
        if (attempt == maxAttempts) {
          throw e; // Rethrow on final attempt
        }
        try {
          // Exponential backoff
          Thread.sleep(initialDelay * (long) Math.pow(2, attempt - 1));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Token request retry interrupted", ie);
        }
        logger.warn("Token request retry attempt {} of {} failed", attempt, maxAttempts, e);
      }
    }
    throw new RuntimeException(
        "Failed to execute token request after " + maxAttempts + " attempts");
  }
}
