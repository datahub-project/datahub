package auth;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.identity.InvitationStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import play.mvc.Http;

@Slf4j
public class AuthUtils {

  /**
   * The config path that determines whether Metadata Service Authentication is enabled.
   *
   * <p>When enabled, the frontend server will proxy requests to the Metadata Service without
   * requiring them to have a valid frontend-issued Session Cookie. This effectively means
   * delegating the act of authentication to the Metadata Service. It is critical that if Metadata
   * Service authentication is enabled at the frontend service layer, it is also enabled in the
   * Metadata Service itself. Otherwise, unauthenticated traffic may reach the Metadata itself.
   *
   * <p>When disabled, the frontend server will require that all requests have a valid Session
   * Cookie associated with them. Otherwise, requests will be denied with an Unauthorized error.
   */
  public static final String METADATA_SERVICE_AUTH_ENABLED_CONFIG_PATH =
      "metadataService.auth.enabled";

  /** The attribute inside session cookie representing a GMS-issued access token */
  public static final String SESSION_COOKIE_GMS_TOKEN_NAME = "token";

  /**
   * An ID used to identify system callers that are internal to DataHub. Provided via configuration.
   */
  public static final String SYSTEM_CLIENT_ID_CONFIG_PATH = "systemClientId";

  /**
   * An Secret used to authenticate system callers that are internal to DataHub. Provided via
   * configuration.
   */
  public static final String SYSTEM_CLIENT_SECRET_CONFIG_PATH = "systemClientSecret";

  /** Cookie name for redirect url that is manually separated from the session to reduce size */
  public static final String REDIRECT_URL_COOKIE_NAME = "REDIRECT_URL";

  public static final CorpuserUrn DEFAULT_ACTOR_URN = new CorpuserUrn("datahub");

  public static final String LOGIN_ROUTE = "/login";
  public static final String USER_NAME = "username";
  public static final String PASSWORD = "password";
  public static final String ACTOR = "actor";
  public static final String ACCESS_TOKEN = "token";
  public static final String FULL_NAME = "fullName";
  public static final String EMAIL = "email";
  public static final String TITLE = "title";
  public static final String INVITE_TOKEN = "inviteToken";
  public static final String RESET_TOKEN = "resetToken";
  public static final String BASE_URL = "baseUrl";
  public static final String OIDC_ENABLED = "oidcEnabled";
  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String DISCOVERY_URI = "discoveryUri";

  public static final String USER_NAME_CLAIM = "userNameClaim";
  public static final String USER_NAME_CLAIM_REGEX = "userNameClaimRegex";
  public static final String SCOPE = "scope";
  public static final String CLIENT_NAME = "clientName";
  public static final String CLIENT_AUTHENTICATION_METHOD = "clientAuthenticationMethod";
  public static final String JIT_PROVISIONING_ENABLED = "jitProvisioningEnabled";
  public static final String PRE_PROVISIONING_REQUIRED = "preProvisioningRequired";
  public static final String EXTRACT_GROUPS_ENABLED = "extractGroupsEnabled";
  public static final String GROUPS_CLAIM = "groupsClaim";
  public static final String RESPONSE_TYPE = "responseType";
  public static final String RESPONSE_MODE = "responseMode";
  public static final String USE_NONCE = "useNonce";
  public static final String READ_TIMEOUT = "readTimeout";
  public static final String CONNECT_TIMEOUT = "connectTimeout";
  public static final String EXTRACT_JWT_ACCESS_TOKEN_CLAIMS = "extractJwtAccessTokenClaims";
  // Retained for backwards compatibility
  public static final String PREFERRED_JWS_ALGORITHM = "preferredJwsAlgorithm";
  public static final String PREFERRED_JWS_ALGORITHM_2 = "preferredJwsAlgorithm2";

  /**
   * Determines whether the inbound request should be forward to downstream Metadata Service. Today,
   * this simply checks for the presence of an "Authorization" header or the presence of a valid
   * session cookie issued by the frontend.
   *
   * <p>Note that this method DOES NOT actually verify the authentication token of an inbound
   * request. That will be handled by the downstream Metadata Service. Until then, the request
   * should be treated as UNAUTHENTICATED.
   *
   * <p>Returns true if the request is eligible to be forwarded to GMS, false otherwise.
   */
  public static boolean isEligibleForForwarding(Http.Request req) {
    return hasValidSessionCookie(req) || hasAuthHeader(req);
  }

  /**
   * Returns true if a request has a valid session cookie issued by the frontend server. Note that
   * this DOES NOT verify whether the token within the session cookie will be accepted by the
   * downstream GMS service.
   *
   * <p>Note that we depend on the presence of 2 cookies, one accessible to the browser and one not,
   * as well as their agreement to determine authentication status.
   */
  public static boolean hasValidSessionCookie(final Http.Request req) {
    Map<String, String> sessionCookie = req.session().data();
    return sessionCookie.containsKey(ACCESS_TOKEN)
        && sessionCookie.containsKey(ACTOR)
        && req.getCookie(ACTOR).isPresent()
        && req.session().data().get(ACTOR).equals(req.getCookie(ACTOR).get().value());
  }

  /** Returns true if a request includes the Authorization header, false otherwise */
  public static boolean hasAuthHeader(final Http.Request req) {
    return req.getHeaders().contains(Http.HeaderNames.AUTHORIZATION);
  }

  /**
   * Creates a client authentication cookie (actor cookie) with a specified TTL in hours.
   *
   * @param actorUrn the urn of the authenticated actor, e.g. "urn:li:corpuser:datahub"
   * @param ttlInHours the number of hours until the actor cookie expires after being set
   */
  public static Http.Cookie createActorCookie(
      @Nonnull final String actorUrn,
      @Nonnull final Integer ttlInHours,
      @Nonnull final String sameSite,
      final boolean isSecure) {
    return Http.Cookie.builder(ACTOR, actorUrn)
        .withHttpOnly(false)
        .withMaxAge(Duration.of(ttlInHours, ChronoUnit.HOURS))
        .withSameSite(convertSameSiteValue(sameSite))
        .withSecure(isSecure)
        .build();
  }

  public static Map<String, String> createSessionMap(
      final String userUrnStr, final String accessToken) {
    final Map<String, String> sessionAttributes = new HashMap<>();
    sessionAttributes.put(ACTOR, userUrnStr);
    sessionAttributes.put(ACCESS_TOKEN, accessToken);
    return sessionAttributes;
  }

  private AuthUtils() {}

  private static Http.Cookie.SameSite convertSameSiteValue(@Nonnull final String sameSiteValue) {
    try {
      return Http.Cookie.SameSite.valueOf(sameSiteValue);
    } catch (IllegalArgumentException e) {
      log.warn(
          String.format(
              "Invalid AUTH_COOKIE_SAME_SITE value: %s. Using LAX instead.", sameSiteValue),
          e);
      return Http.Cookie.SameSite.LAX;
    }
  }

  public static void tryProvisionUser(
      @Nonnull OperationContext opContext,
      CorpUserSnapshot corpUserSnapshot,
      SystemEntityClient systemEntityClient) {

    log.debug(String.format("Attempting to provision user with urn %s", corpUserSnapshot.getUrn()));

    // 1. Check if this user already exists.
    try {
      final Entity corpUser = systemEntityClient.get(opContext, corpUserSnapshot.getUrn());
      final CorpUserSnapshot existingCorpUserSnapshot = corpUser.getValue().getCorpUserSnapshot();

      log.debug(String.format("Fetched GMS user with urn %s", corpUserSnapshot.getUrn()));

      // Check if we should provision the user (either they don't exist or only have SENT invitation
      // status)
      if (shouldProvisionUserWithExistingAspects(
          opContext, corpUserSnapshot.getUrn(), existingCorpUserSnapshot, systemEntityClient)) {
        log.debug(
            String.format(
                "Extracted user that needs provisioning %s. Provisioning...",
                corpUserSnapshot.getUrn()));
        // 2. The user does not exist or only has a SENT invitation. Provision them.
        final Entity newEntity = new Entity();
        newEntity.setValue(Snapshot.create(corpUserSnapshot));
        systemEntityClient.update(opContext, newEntity);
        log.debug(String.format("Successfully provisioned user %s", corpUserSnapshot.getUrn()));

        // 3. Ensure newly provisioned user is active (JIT provisioning)
        try {
          ensureUserIsActive(opContext, corpUserSnapshot.getUrn(), systemEntityClient);
        } catch (Exception e) {
          log.error("Failed to ensure user is active: {}", e.getMessage(), e);
          throw new RuntimeException("Failed to ensure user is active", e);
        }
      } else {
        log.debug(
            String.format(
                "User %s already exists. Skipping provisioning", corpUserSnapshot.getUrn()));
      }

    } catch (RemoteInvocationException e) {
      // Failing provisioning is something worth throwing about.
      throw new RuntimeException(
          String.format("Failed to provision user with urn %s.", corpUserSnapshot.getUrn()), e);
    }
  }

  /**
   * Check if user status is active
   *
   * <p>Note: This method is primarily used internally by ensureUserIsActive() and for testing. For
   * most use cases, prefer using tryProvisionUser() which includes user activation.
   *
   * @param opContext Operation context
   * @param userUrn User URN to check
   * @param entityClient Entity client to use for the query
   * @return true if user is active, false otherwise
   * @throws Exception if there's an error checking user status
   */
  public static boolean checkIsUserStatusActive(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn userUrn,
      @Nonnull SystemEntityClient entityClient)
      throws Exception {
    final EntityResponse response =
        entityClient.getV2(
            opContext, userUrn, Collections.singleton(Constants.CORP_USER_STATUS_ASPECT_NAME));
    if (response != null && response.hasAspects()) {
      final EnvelopedAspect aspect =
          response.getAspects().get(Constants.CORP_USER_STATUS_ASPECT_NAME);
      final CorpUserStatus status = new CorpUserStatus(aspect.getValue().data());
      return status.hasStatus() && status.getStatus().equals(Constants.CORP_USER_STATUS_ACTIVE);
    }
    return false;
  }

  /**
   * Set user status to active
   *
   * <p>Note: This method is primarily used internally by ensureUserIsActive() and for testing. For
   * most use cases, prefer using tryProvisionUser() which includes user activation.
   *
   * @param opContext Operation context
   * @param urn User URN to update
   * @param newStatus New user status
   * @param entityClient Entity client to use for the update
   * @throws Exception if there's an error setting user status
   */
  public static void setUserStatus(
      @Nonnull OperationContext opContext,
      final Urn urn,
      final CorpUserStatus newStatus,
      @Nonnull SystemEntityClient entityClient)
      throws Exception {
    // Update status aspect to be active.
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
    proposal.setAspectName(Constants.CORP_USER_STATUS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newStatus));
    proposal.setChangeType(ChangeType.UPSERT);
    entityClient.ingestProposal(opContext, proposal, true);
  }

  /**
   * Ensure user is active - checks if user status is active and sets it to active if not
   *
   * <p>Note: This method is primarily used internally by tryProvisionUser() and for testing. For
   * most use cases, prefer using tryProvisionUser() which includes user activation.
   *
   * @param opContext Operation context
   * @param userUrn User URN to check and potentially update
   * @param entityClient Entity client to use for the operations
   * @throws Exception if there's an error checking or setting user status
   */
  public static void ensureUserIsActive(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn userUrn,
      @Nonnull SystemEntityClient entityClient)
      throws Exception {
    if (!checkIsUserStatusActive(opContext, userUrn, entityClient)) {
      log.info("User {} is not active, updating status.", userUrn);
      setUserStatus(
          opContext,
          userUrn,
          new CorpUserStatus()
              .setStatus(Constants.CORP_USER_STATUS_ACTIVE)
              .setLastModified(
                  new AuditStamp()
                      .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                      .setTime(System.currentTimeMillis())),
          entityClient);
    }
  }

  /**
   * Helper method to check if a user should be provisioned even though they have existing aspects.
   * This allows provisioning when the user has a CorpUserInvitationStatus with SENT status.
   *
   * @param opContext Operation context
   * @param corpUserUrn The user URN
   * @param existingCorpUserSnapshot The existing user snapshot
   * @param systemEntityClient Entity client to check invitation status
   * @return true if the user should be provisioned, false otherwise
   */
  private static boolean shouldProvisionUserWithExistingAspects(
      @Nonnull OperationContext opContext,
      @Nonnull CorpuserUrn corpUserUrn,
      CorpUserSnapshot existingCorpUserSnapshot,
      @Nonnull SystemEntityClient systemEntityClient) {

    // If only the key aspect exists, we should provision
    if (existingCorpUserSnapshot.getAspects().size() <= 1) {
      return true;
    }

    // Check if the user has a SENT invitation status by making a direct call to the entity client
    try {
      Aspect invitationAspectObject =
          systemEntityClient.getLatestAspectObject(
              opContext, corpUserUrn, Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME, false);

      if (invitationAspectObject != null) {
        CorpUserInvitationStatus invitationStatus =
            new CorpUserInvitationStatus(invitationAspectObject.data());
        if (invitationStatus.getStatus() == InvitationStatus.SENT) {
          log.debug("User {} has SENT invitation status, allowing SSO provisioning", corpUserUrn);
          return true;
        } else {
          log.debug(
              "User {} has invitation status {}, not allowing SSO provisioning",
              corpUserUrn,
              invitationStatus.getStatus());
        }
      }
    } catch (Exception e) {
      log.warn("Failed to check CorpUserInvitationStatus for user {}", corpUserUrn, e);
    }

    return false;
  }

  /**
   * Verify that a pre-provisioned user exists in the system
   *
   * <p>This method validates that a user account has been pre-created by an admin and is ready for
   * use. It checks that the user has more than just the key aspect, indicating it has been properly
   * provisioned.
   *
   * @param opContext Operation context
   * @param userUrn User URN to verify
   * @param entityClient Entity client to use for the verification
   * @throws RuntimeException if user doesn't exist or validation fails
   */
  public static void verifyPreProvisionedUser(
      @Nonnull OperationContext opContext,
      @Nonnull CorpuserUrn userUrn,
      @Nonnull SystemEntityClient entityClient) {
    // Validate that the user exists in the system (there is more than just a key aspect for them,
    // as of today).
    try {
      final Entity corpUser = entityClient.get(opContext, userUrn);

      log.debug("Fetched GMS user with urn {}", userUrn);

      // If we find more than the key aspect, then the entity "exists".
      if (corpUser.getValue().getCorpUserSnapshot().getAspects().size() <= 1) {
        log.debug(
            "Found user that does not yet exist {}. Invalid login attempt. Throwing...", userUrn);
        throw new RuntimeException(
            String.format(
                "User with urn %s has not yet been provisioned in DataHub. "
                    + "Please contact your DataHub admin to provision an account.",
                userUrn));
      }
      // Otherwise, the user exists.
    } catch (RemoteInvocationException e) {
      // Failing validation is something worth throwing about.
      throw new RuntimeException(String.format("Failed to validate user with urn %s.", userUrn), e);
    }
  }
}
