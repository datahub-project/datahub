package auth.sso.oidc.custom;

import auth.sso.oidc.OidcConfigs;
import auth.sso.oidc.PrivateKeyJwtUtils;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.ClientSecretPost;
import com.nimbusds.oauth2.sdk.auth.JWTAuthenticationClaimsSet;
import com.nimbusds.oauth2.sdk.auth.PrivateKeyJWT;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.Audience;
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
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
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

  private static final Set<ClientAuthenticationMethod> SUPPORTED_METHODS =
      Set.of(
          ClientAuthenticationMethod.CLIENT_SECRET_POST,
          ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
          ClientAuthenticationMethod.PRIVATE_KEY_JWT,
          ClientAuthenticationMethod.NONE);

  // Nimbus's JWSAlgorithm.parse silently accepts unknown names and returns a placeholder that
  // explodes later inside the signer; we validate eagerly against this allow-list at startup.
  private static final Set<JWSAlgorithm> SUPPORTED_PRIVATE_KEY_JWT_ALGORITHMS =
      Set.of(JWSAlgorithm.RS256, JWSAlgorithm.RS384, JWSAlgorithm.RS512);

  private final OidcConfigs oidcConfigs;
  private final ClientAuthenticationMethod chosenMethod;
  private final ClientID clientID;
  private final URI tokenEndpoint;

  /** Pre-parsed signing material; {@code null} unless {@link #chosenMethod} is private_key_jwt. */
  @Nullable private final PrivateKeyJwtMaterial pkjMaterial;

  public CustomOidcAuthenticator(final OidcClient client, final OidcConfigs oidcConfigs) {
    super(client.getConfiguration(), client);
    this.oidcConfigs = oidcConfigs;

    OIDCProviderMetadata providerMetadata;
    try {
      providerMetadata = loadWithRetry();
    } catch (TechnicalException e) {
      logger.error(
          "Could not resolve identity provider's remote configuration from DiscoveryURI: {}",
          configuration.getDiscoveryURI());
      throw e;
    }

    final List<ClientAuthenticationMethod> metadataMethods =
        providerMetadata.getTokenEndpointAuthMethods();
    final ClientAuthenticationMethod preferredMethod =
        getPreferredAuthenticationMethod(configuration);

    if (CommonHelper.isNotEmpty(metadataMethods)) {
      if (preferredMethod != null) {
        if (!ClientAuthenticationMethod.NONE.equals(preferredMethod)
            && !metadataMethods.contains(preferredMethod)) {
          throw new TechnicalException(
              "Preferred authentication method ("
                  + preferredMethod
                  + ") not supported by provider according to provider metadata ("
                  + metadataMethods
                  + ").");
        }
        this.chosenMethod = preferredMethod;
      } else {
        this.chosenMethod = firstSupportedMethod(metadataMethods);
      }
    } else {
      this.chosenMethod =
          preferredMethod != null ? preferredMethod : ClientAuthenticationMethod.getDefault();
      logger.info(
          "Provider metadata does not provide Token endpoint authentication methods. Using: {}",
          chosenMethod);
    }

    this.clientID = new ClientID(configuration.getClientId());
    // Cached at startup: token endpoint rollovers are rare and require operator action anyway,
    // so the restart-to-refresh trade-off avoids a metadata-resolver hit per login.
    this.tokenEndpoint = providerMetadata.getTokenEndpointURI();
    this.pkjMaterial =
        ClientAuthenticationMethod.PRIVATE_KEY_JWT.equals(chosenMethod)
            ? loadPrivateKeyJwtMaterial(oidcConfigs)
            : null;
  }

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

  private static ClientAuthenticationMethod firstSupportedMethod(
      final List<ClientAuthenticationMethod> metadataMethods) {
    return metadataMethods.stream()
        .filter(SUPPORTED_METHODS::contains)
        .findFirst()
        .orElseThrow(
            () ->
                new TechnicalException(
                    "None of the Token endpoint provider metadata authentication methods are supported: "
                        + metadataMethods));
  }

  /**
   * Builds a fresh {@link TokenRequest} per call. A new {@link ClientAuthentication} is constructed
   * every time so that {@code private_key_jwt} assertions always have a future {@code exp} claim —
   * caching would let them go stale within minutes of uptime.
   */
  @Override
  protected TokenRequest createTokenRequest(AuthorizationGrant grant) {
    final Scope scope = Scope.parse(configuration.getScope());
    try {
      final ClientAuthentication clientAuth = buildClientAuthentication();
      return clientAuth == null
          ? new TokenRequest(tokenEndpoint, clientID, grant, scope)
          : new TokenRequest(tokenEndpoint, clientAuth, grant, scope);
    } catch (JOSEException e) {
      throw new TechnicalException("Failed to sign private_key_jwt client assertion", e);
    }
  }

  @Nullable
  private ClientAuthentication buildClientAuthentication() throws JOSEException {
    if (ClientAuthenticationMethod.CLIENT_SECRET_POST.equals(chosenMethod)) {
      return new ClientSecretPost(clientID, new Secret(configuration.getSecret()));
    }
    if (ClientAuthenticationMethod.CLIENT_SECRET_BASIC.equals(chosenMethod)) {
      return new ClientSecretBasic(clientID, new Secret(configuration.getSecret()));
    }
    if (ClientAuthenticationMethod.PRIVATE_KEY_JWT.equals(chosenMethod)) {
      return signFreshPrivateKeyJwt();
    }
    if (ClientAuthenticationMethod.NONE.equals(chosenMethod)) {
      return null;
    }
    throw new TechnicalException("Unsupported client authentication method: " + chosenMethod);
  }

  /**
   * Signs a brand-new {@code private_key_jwt} client assertion (RFC 7523). The header carries
   * {@code kid}, {@code x5t#S256} and {@code x5c} so any OIDC-compliant IdP can match on whichever
   * key-identification field it prefers; {@link JWTAuthenticationClaimsSet} generates a fresh
   * {@code jti}, {@code iat} and {@code exp} on every invocation.
   */
  private ClientAuthentication signFreshPrivateKeyJwt() throws JOSEException {
    JWSHeader header =
        new JWSHeader.Builder(pkjMaterial.algorithm())
            .keyID(pkjMaterial.kid())
            .x509CertSHA256Thumbprint(pkjMaterial.x5tS256())
            .x509CertChain(pkjMaterial.x5c())
            .build();
    JWTAuthenticationClaimsSet claims =
        new JWTAuthenticationClaimsSet(clientID, new Audience(tokenEndpoint.toString()));
    SignedJWT jwt = new SignedJWT(header, claims.toJWTClaimsSet());
    jwt.sign(new RSASSASigner(pkjMaterial.privateKey()));
    return new PrivateKeyJWT(jwt);
  }

  /** Loads the PEM key + cert chain once at startup and pre-computes header fields. */
  private static PrivateKeyJwtMaterial loadPrivateKeyJwtMaterial(OidcConfigs oidcConfigs) {
    try {
      String privateKeyPath =
          oidcConfigs
              .getPrivateKeyFilePath()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "privateKeyFilePath is required for private_key_jwt authentication"));
      String certificatePath =
          oidcConfigs
              .getCertificateFilePath()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "certificateFilePath is required for private_key_jwt authentication"));

      PrivateKey privateKey =
          PrivateKeyJwtUtils.loadPrivateKey(
              privateKeyPath, oidcConfigs.getPrivateKeyPassword().orElse(null));
      List<X509Certificate> chain = PrivateKeyJwtUtils.loadCertificateChain(certificatePath);
      String thumbprintSha256 = PrivateKeyJwtUtils.computeSha256Thumbprint(chain.get(0));
      String kid = oidcConfigs.getPrivateKeyJwtKid().orElse(thumbprintSha256);

      JWSAlgorithm algorithm = JWSAlgorithm.parse(oidcConfigs.getPrivateKeyJwtAlgorithm());
      if (!SUPPORTED_PRIVATE_KEY_JWT_ALGORITHMS.contains(algorithm)) {
        throw new IllegalArgumentException(
            "Unsupported private_key_jwt algorithm '"
                + algorithm
                + "'. Supported values: "
                + SUPPORTED_PRIVATE_KEY_JWT_ALGORITHMS);
      }

      List<com.nimbusds.jose.util.Base64> x5c = new ArrayList<>(chain.size());
      for (X509Certificate c : chain) {
        x5c.add(com.nimbusds.jose.util.Base64.encode(c.getEncoded()));
      }

      logger.info(
          "Loaded private_key_jwt signing material (alg={}, kid={}, x5t#S256={}, chainLength={})",
          algorithm,
          kid,
          thumbprintSha256,
          chain.size());
      return new PrivateKeyJwtMaterial(
          privateKey, algorithm, kid, new Base64URL(thumbprintSha256), List.copyOf(x5c));
    } catch (IOException | CertificateException | IllegalArgumentException e) {
      throw new TechnicalException(
          "Failed to load private_key_jwt signing material: " + e.getMessage(), e);
    }
  }

  @Override
  public Optional<Credentials> validate(CallContext ctx, Credentials cred) {
    OidcCredentials credentials = (OidcCredentials) cred;
    WebContext context = ctx.webContext();

    final AuthorizationCode code = credentials.toAuthorizationCode();
    if (code != null) {
      try {
        final String computedCallbackUrl = client.computeFinalCallbackUrl(context);
        CodeVerifier verifier =
            (CodeVerifier)
                configuration
                    .getValueRetriever()
                    .retrieve(ctx, client.getCodeVerifierSessionAttributeName(), client)
                    .orElse(null);

        final OIDCTokenResponse tokenSuccessResponse =
            executeTokenRequestWithRetry(code, computedCallbackUrl, verifier);

        final OIDCTokens oidcTokens = tokenSuccessResponse.getOIDCTokens();
        credentials.setAccessTokenObject(oidcTokens.getAccessToken());
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
  protected OIDCProviderMetadata loadWithRetry() {
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

  /** Pre-parsed signing material for {@code private_key_jwt}, immutable after startup. */
  private record PrivateKeyJwtMaterial(
      PrivateKey privateKey,
      JWSAlgorithm algorithm,
      String kid,
      Base64URL x5tS256,
      List<com.nimbusds.jose.util.Base64> x5c) {}
}
