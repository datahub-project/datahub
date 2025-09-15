package controllers;

import static auth.AuthUtils.*;
import static auth.sso.SsoConfigs.OIDC_ENABLED_CONFIG_PATH;
import static auth.sso.oidc.OidcConfigs.OIDC_IMPLICIT_ENABLED;
import static utils.FrontendConstants.OIDC_IMPLICIT_LOGIN;

import auth.AuthUtils;
import auth.CookieConfigs;
import auth.sso.oidc.OidcConfigs;
import client.AuthServiceClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import com.typesafe.config.Config;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Date;
import java.util.List;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

/**
 * Exchanges implicit oidc token for session cookie with client-side only validation This approach
 * is used when the identity provider endpoints are not accessible from the backend controller.
 */
@Slf4j
public class OidcImplicitController extends Controller {

  public static final String AUTH_VERBOSE_LOGGING = "auth.verbose.logging";
  private static final String OIDC_DISABLED_ERROR_MESSAGE = "OIDC Implicit Flow is not configured";

  private static final Logger logger =
      LoggerFactory.getLogger(OidcImplicitController.class.getName());
  private final CookieConfigs cookieConfigs;
  private final OidcConfigs oidcConfigs;
  private final boolean verbose;
  private final com.nimbusds.jose.jwk.JWKSet jwkSet;

  @Inject AuthServiceClient authClient;
  @Inject OidcClient oidcClient;

  @Inject
  @Named("systemOperationContext")
  @Nonnull
  OperationContext systemOperationContext;

  @Inject @Nonnull SystemEntityClient entityClient;

  @Inject
  public OidcImplicitController(@Nonnull Config configs) {
    if (enableOidcImplicitController(configs)) {
      cookieConfigs = new CookieConfigs(configs);
      oidcConfigs = new OidcConfigs.Builder().from(configs).build();
      verbose = configs.hasPath(AUTH_VERBOSE_LOGGING) && configs.getBoolean(AUTH_VERBOSE_LOGGING);

      // Initialize JWKS from configuration
      com.nimbusds.jose.jwk.JWKSet loadedJwkSet = null;
      try {
        String jwksJson = oidcConfigs.getJwksJson();
        if (!StringUtils.isBlank(jwksJson)) {
          loadedJwkSet = com.nimbusds.jose.jwk.JWKSet.parse(jwksJson);
          if (verbose) {
            logger.debug(
                "Successfully loaded JWKS from configuration with {} keys",
                loadedJwkSet.getKeys().size());
          }
        } else {
          logger.error("No JWKS configured.");
        }
      } catch (Exception e) {
        logger.error("Error initializing JWKS from configuration", e);
      }
      this.jwkSet = loadedJwkSet;

      if (verbose) {
        logger.debug(
            "OIDC Configuration - ClientID: {}, ImplicitFlow: {}, JWKS Available: {}",
            oidcConfigs.getClientId(),
            oidcConfigs.isImplicitFlow(),
            (jwkSet != null));
      }
    } else {
      cookieConfigs = null;
      oidcConfigs = null;
      verbose = false;
      jwkSet = null;
    }
  }

  private static boolean enableOidcImplicitController(@Nonnull Config configs) {
    return configs.hasPath(OIDC_ENABLED_CONFIG_PATH)
        && configs.getBoolean(OIDC_ENABLED_CONFIG_PATH)
        && configs.hasPath(OIDC_IMPLICIT_ENABLED)
        && configs.getBoolean(OIDC_IMPLICIT_ENABLED);
  }

  /**
   * API endpoint to exchange the OIDC tokens for a session. This is called by client-side
   * JavaScript after receiving tokens from the IdP.
   */
  @Nonnull
  public Result exchangeTokenForSession(Http.Request request) {
    if (oidcConfigs == null || !oidcConfigs.isImplicitFlow()) {
      ObjectNode error = Json.newObject();
      error.put("message", OIDC_DISABLED_ERROR_MESSAGE);
      return Results.badRequest(error);
    }

    final JsonNode json = request.body().asJson();
    if (json == null) {
      ObjectNode error = Json.newObject();
      error.put("message", "No JSON body provided");
      return Results.badRequest(error);
    }

    final String idToken = json.findPath("id_token").textValue();
    final String accessToken = json.findPath("access_token").textValue();

    if (StringUtils.isBlank(idToken) || StringUtils.isBlank(accessToken)) {
      ObjectNode error = Json.newObject();
      error.put("message", "ID token and access token are required");
      return Results.badRequest(error);
    }

    try {
      // Create OidcCredentials with the tokens
      OidcCredentials credentials = new OidcCredentials();
      credentials.setIdToken(idToken);

      // Validate the ID token using the JWKS from configuration
      JWTClaimsSet claimsSet = validateIdToken(idToken);
      if (claimsSet == null) {
        ObjectNode error = Json.newObject();
        error.put("message", "ID token validation failed");
        return Results.badRequest(error);
      }

      // Extract user information from validated claims
      String email = extractEmailFromClaims(claimsSet);

      if (StringUtils.isBlank(email)) {
        ObjectNode error = Json.newObject();
        error.put("message", "Could not extract email from ID token");
        return Results.badRequest(error);
      }

      if (verbose) {
        logger.debug("Successfully validated token and extracted email: {}", email);
      }

      // Create user URN (trim email to avoid whitespace issues)
      final Urn userUrn = new CorpuserUrn(email.trim());
      final String userUrnString = userUrn.toString();

      // If just-in-time User Provisioning is enabled, try to create the DataHub user if it does not
      // exist.
      if (oidcConfigs.isJitProvisioningEnabled()) {
        if (verbose) {
          logger.debug("Just-in-time provisioning is enabled. Beginning provisioning process...");
        }
        CorpUserSnapshot extractedUser = extractUser((CorpuserUrn) userUrn, claimsSet, email);
        provisionUser(systemOperationContext, extractedUser, entityClient);
      } else if (oidcConfigs.isPreProvisioningRequired()) {
        // We should only allow logins for user accounts that have been pre-provisioned
        if (verbose) {
          logger.debug("Pre Provisioning is required. Beginning validation of extracted user...");
        }
        AuthUtils.verifyPreProvisionedUser(
            systemOperationContext, (CorpuserUrn) userUrn, entityClient);
      }

      // Generate session token
      final String sessionToken =
          authClient.generateSessionTokenForUser(userUrn.getId(), OIDC_IMPLICIT_LOGIN);

      // Create session
      return Results.ok()
          .withSession(createSessionMap(userUrnString, sessionToken))
          .withCookies(
              createActorCookie(
                  userUrnString,
                  cookieConfigs.getTtlInHours(),
                  cookieConfigs.getAuthCookieSameSite(),
                  cookieConfigs.getAuthCookieSecure()));
    } catch (Exception e) {
      logger.error("Error processing OIDC tokens", e);
      ObjectNode error = Json.newObject();
      error.put("message", "Failed to process OIDC tokens: " + e.getMessage());
      return Results.badRequest(error);
    }
  }

  /**
   * Validate the ID token using the JWKS from configuration
   *
   * @param idToken The ID token to validate
   * @return The parsed JWT claims if validation succeeds, null otherwise
   */
  private JWTClaimsSet validateIdToken(String idToken) {
    try {
      // Parse the JWT to extract basic claims for initial validation
      JWT jwt = JWTParser.parse(idToken);
      JWTClaimsSet claimsSet = jwt.getJWTClaimsSet();

      // Validate basic claims
      if (!validateBasicClaims(claimsSet)) {
        return null;
      }

      // If JWKS is available, validate the signature
      if (jwkSet != null && jwt instanceof SignedJWT) {
        if (!validateSignatureWithJwks((SignedJWT) jwt)) {
          logger.error("Token signature validation failed");
          return null;
        }
      } else {
        // If we can't validate the signature, log a warning
        logger.error("Signature error validation due to missing JWKS");
        return null;
      }

      return claimsSet;

    } catch (Exception e) {
      logger.error("Error parsing/validating ID token", e);
      return null;
    }
  }

  /** Validate basic claims in the token that don't require IdP access */
  private boolean validateBasicClaims(JWTClaimsSet claimsSet) {
    try {
      // Current time with some allowance for clock skew
      Date now = new Date();
      int clockSkewSeconds = 30; // Default clock skew

      if (oidcClient != null && oidcClient.getConfiguration() != null) {
        clockSkewSeconds = oidcClient.getConfiguration().getMaxClockSkew();
      }

      String clientId = oidcConfigs.getClientId();
      String expectedIssuer = oidcConfigs.getClientIssuer();

      // 1. Check issuer if we have an expected one
      if (expectedIssuer != null && !expectedIssuer.equals(claimsSet.getIssuer())) {
        logger.error("Invalid issuer: expected {}, got {}", expectedIssuer, claimsSet.getIssuer());
        return false;
      }

      // 2. Check audience
      List<String> audiences = claimsSet.getAudience();
      if (audiences == null || !audiences.contains(clientId)) {
        logger.error("Invalid audience: expected {}, got {}", clientId, audiences);
        return false;
      }

      // 3. Check expiration time
      Date expirationTime = claimsSet.getExpirationTime();
      if (expirationTime == null) {
        logger.error("Token missing expiration claim");
        return false;
      }

      // Add clock skew to expiration time
      Date expWithSkew = new Date(expirationTime.getTime() + clockSkewSeconds * 1000L);
      if (now.after(expWithSkew)) {
        logger.error("Token expired at {}, current time: {}", expirationTime, now);
        return false;
      }

      // 4. Check not-before time if present
      Date nbf = claimsSet.getNotBeforeTime();
      if (nbf != null) {
        // Subtract clock skew from not-before time
        Date nbfWithSkew = new Date(nbf.getTime() - clockSkewSeconds * 1000L);
        if (now.before(nbfWithSkew)) {
          logger.error("Token not valid before {}, current time: {}", nbf, now);
          return false;
        }
      }

      // 5. Check issued-at-time if present
      Date iat = claimsSet.getIssueTime();
      if (iat != null) {
        // Check if token was issued too far in the future
        Date iatWithSkew = new Date(iat.getTime() - clockSkewSeconds * 1000L);
        if (now.before(iatWithSkew)) {
          logger.error("Token issued at future time {}, current time: {}", iat, now);
          return false;
        }
      }

      return true;
    } catch (Exception e) {
      logger.error("Error validating token claims", e);
      return false;
    }
  }

  /** Validate the token signature using the JWKS from configuration */
  private boolean validateSignatureWithJwks(SignedJWT jwt) {
    try {
      // Get the key ID from the JWT header
      String keyId = jwt.getHeader().getKeyID();
      if (keyId == null) {
        logger.error("No key ID found in JWT header");
        return false;
      }

      // Find the JWK with the matching key ID
      com.nimbusds.jose.jwk.JWK jwk = jwkSet.getKeyByKeyId(keyId);
      if (jwk == null) {
        logger.error("No matching JWK found for key ID: {}", keyId);
        return false;
      }

      // Create a verifier based on the key type
      com.nimbusds.jose.JWSVerifier verifier;

      if (jwk instanceof com.nimbusds.jose.jwk.RSAKey) {
        // RSA key
        verifier =
            new com.nimbusds.jose.crypto.RSASSAVerifier(
                ((com.nimbusds.jose.jwk.RSAKey) jwk).toRSAPublicKey());
      } else if (jwk instanceof com.nimbusds.jose.jwk.ECKey) {
        // EC key
        verifier =
            new com.nimbusds.jose.crypto.ECDSAVerifier(
                ((com.nimbusds.jose.jwk.ECKey) jwk).toECPublicKey());
      } else {
        logger.error("Unsupported key type: {}", jwk.getClass().getName());
        return false;
      }

      // Verify the signature
      return jwt.verify(verifier);

    } catch (Exception e) {
      logger.error("Error validating token signature with JWKS", e);
      return false;
    }
  }

  /** Extract email from validated JWT claims */
  private String extractEmailFromClaims(JWTClaimsSet claims) {
    try {
      // Try standard claims for email
      String email = claims.getStringClaim("email");

      // If email claim not present, try other common claims
      if (StringUtils.isBlank(email)) {
        // Some providers use upn (User Principal Name)
        email = claims.getStringClaim("upn");
      }

      // Try preferred_username
      if (StringUtils.isBlank(email)) {
        email = claims.getStringClaim("preferred_username");
      }

      // If still not found, use the subject as a last resort
      if (StringUtils.isBlank(email)) {
        email = claims.getSubject();
      }

      return email;
    } catch (Exception e) {
      logger.error("Error extracting email from claims", e);
      return null;
    }
  }

  public CorpUserSnapshot extractUser(CorpuserUrn urn, JWTClaimsSet claims, String email) {
    log.debug(String.format("Attempting to extract user from JWT claims %s", claims.toString()));
    final CorpUserSnapshot corpUserSnapshot = new CorpUserSnapshot();

    try {
      String firstName = claims.getClaimAsString("given_name");
      String lastName = claims.getClaimAsString("family_name");
      String fullName = null;
      if (firstName != null && lastName != null) {
        fullName = String.format("%s %s", firstName, lastName);
      }

      // Create the CorpUserInfo aspect with the available information
      final CorpUserInfo userInfo = new CorpUserInfo();
      userInfo.setActive(true);
      userInfo.setFirstName(firstName, SetMode.IGNORE_NULL);
      userInfo.setLastName(lastName, SetMode.IGNORE_NULL);
      userInfo.setEmail(email, SetMode.IGNORE_NULL);
      userInfo.setDisplayName(fullName, SetMode.IGNORE_NULL);

      // Create the CorpUserSnapshot
      corpUserSnapshot.setUrn(urn);
      final CorpUserAspectArray aspects = new CorpUserAspectArray();
      aspects.add(CorpUserAspect.create(userInfo));
      corpUserSnapshot.setAspects(aspects);
    } catch (Exception e) {
      log.error("Error extracting user details from claims", e);
    }

    return corpUserSnapshot;
  }

  /** Protected method to provision user - can be mocked in tests */
  protected void provisionUser(
      @Nonnull OperationContext opContext,
      CorpUserSnapshot corpUserSnapshot,
      SystemEntityClient systemEntityClient) {
    AuthUtils.tryProvisionUser(opContext, corpUserSnapshot, systemEntityClient);
  }
}
