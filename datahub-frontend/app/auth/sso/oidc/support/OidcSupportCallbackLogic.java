package auth.sso.oidc.support;

import static auth.AuthUtils.*;
import static auth.AuthUtils.REDIRECT_URL_COOKIE_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static org.pac4j.play.store.PlayCookieSessionStore.*;
import static play.mvc.Results.internalServerError;
import static utils.FrontendConstants.SSO_LOGIN;

import auth.AuthUtils;
import auth.CookieConfigs;
import auth.sso.SsoSupportManager;
import auth.sso.oidc.OidcResponseErrorHandler;
import client.AuthServiceClient;
import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.template.NotificationTemplateType;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.aspect.CorpGroupAspectArray;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.core.client.BaseClient;
import org.pac4j.core.client.Client;
import org.pac4j.core.client.Clients;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.Cookie;
import org.pac4j.core.context.FrameworkParameters;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.credentials.Credentials;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.exception.http.HttpAction;
import org.pac4j.core.http.adapter.HttpActionAdapter;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.core.util.CommonHelper;
import org.pac4j.core.util.Pac4jConstants;
import org.pac4j.play.store.PlayCookieSessionStore;
import play.mvc.Http;
import play.mvc.Result;
import utils.AcrylConstants;
import utils.ConfigUtil;

/**
 * This class contains the logic that is executed when an OpenID Connect Identity Provider redirects
 * back to DataHub after an authentication attempt for support staff.
 *
 * <p>On receiving a user profile from the IdP (using /userInfo endpoint), we attempt to extract
 * basic information about the user including their name, email, etc. For support staff, we always
 * enable JIT provisioning with a hardcoded group and role extracted from OIDC claims.
 */
@Slf4j
public class OidcSupportCallbackLogic extends DefaultCallbackLogic {

  private final SsoSupportManager ssoSupportManager;
  private final SystemEntityClient systemEntityClient;
  private final OperationContext systemOperationContext;
  private final AuthServiceClient authClient;
  private final CookieConfigs cookieConfigs;
  private final com.typesafe.config.Config config;
  private final HttpClient httpClient;

  public OidcSupportCallbackLogic(
      final SsoSupportManager ssoSupportManager,
      final OperationContext systemOperationContext,
      final SystemEntityClient entityClient,
      final AuthServiceClient authClient,
      final CookieConfigs cookieConfigs,
      final com.typesafe.config.Config config) {
    this.ssoSupportManager = ssoSupportManager;
    this.systemOperationContext = systemOperationContext;
    systemEntityClient = entityClient;
    this.authClient = authClient;
    this.cookieConfigs = cookieConfigs;
    this.config = config;
    this.httpClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .version(java.net.http.HttpClient.Version.HTTP_1_1)
            .build();
  }

  @Override
  public Object perform(
      org.pac4j.core.config.Config config,
      String inputDefaultUrl,
      Boolean inputRenewSession,
      String defaultClient,
      FrameworkParameters parameters) {

    final Pair<CallContext, Object> ctxResult =
        superPerform(config, inputDefaultUrl, inputRenewSession, defaultClient, parameters);

    CallContext ctx = ctxResult.getFirst();
    Result result = (Result) ctxResult.getSecond();

    // Handle OIDC authentication errors.
    if (OidcResponseErrorHandler.isError(ctx)) {
      return OidcResponseErrorHandler.handleError(ctx);
    }

    // By this point, we know that OIDC Support is the enabled provider.
    final OidcSupportConfigs oidcSupportConfigs =
        (OidcSupportConfigs) ssoSupportManager.getSupportSsoProvider().configs();
    return handleOidcSupportCallback(systemOperationContext, ctx, oidcSupportConfigs, result);
  }

  /** Overriding this to be able to intercept the CallContext being created */
  private Pair<CallContext, Object> superPerform(
      Config config,
      String inputDefaultUrl,
      Boolean inputRenewSession,
      String defaultClient,
      FrameworkParameters parameters) {
    log.debug("=== SUPPORT CALLBACK ===");
    CallContext ctx = this.buildContext(config, parameters);
    WebContext webContext = ctx.webContext();
    HttpActionAdapter httpActionAdapter = config.getHttpActionAdapter();
    CommonHelper.assertNotNull("httpActionAdapter", httpActionAdapter);

    HttpAction action;
    try {
      CommonHelper.assertNotNull("clientFinder", getClientFinder());
      String defaultUrl = (String) Objects.requireNonNullElse(inputDefaultUrl, "/");
      boolean renewSession = inputRenewSession == null || inputRenewSession;
      CommonHelper.assertNotBlank("defaultUrl", defaultUrl);
      Clients clients = config.getClients();
      CommonHelper.assertNotNull("clients", clients);
      List<Client> foundClients = getClientFinder().find(clients, webContext, defaultClient);
      CommonHelper.assertTrue(
          foundClients != null && foundClients.size() == 1,
          "unable to find one indirect client for the callback: check the callback URL for a client name parameter or suffix path or ensure that your configuration defaults to one indirect client");
      Client foundClient = (Client) foundClients.get(0);
      log.debug("foundClient: {}", foundClient);
      CommonHelper.assertNotNull("foundClient", foundClient);
      Credentials credentials = (Credentials) foundClient.getCredentials(ctx).orElse(null);
      log.debug("extracted credentials: {}", credentials);
      credentials = (Credentials) foundClient.validateCredentials(ctx, credentials).orElse(null);
      log.debug("validated credentials: {}", credentials);
      if (credentials != null && !credentials.isForAuthentication()) {
        action = foundClient.processLogout(ctx, credentials);
      } else {
        if (credentials != null) {
          Optional<UserProfile> optProfile = foundClient.getUserProfile(ctx, credentials);
          log.debug("optProfile: {}", optProfile);
          if (optProfile.isPresent()) {
            UserProfile profile = (UserProfile) optProfile.get();
            Boolean saveProfileInSession =
                ((BaseClient) foundClient).getSaveProfileInSession(webContext, profile);
            boolean multiProfile = ((BaseClient) foundClient).isMultiProfile(webContext, profile);
            log.debug(
                "saveProfileInSession: {} / multiProfile: {}", saveProfileInSession, multiProfile);
            this.saveUserProfile(
                ctx, config, profile, saveProfileInSession, multiProfile, renewSession);
          }
        }

        // Set the redirect url from cookie before creating action
        setContextRedirectUrl(ctx);

        action = this.redirectToOriginallyRequestedUrl(ctx, defaultUrl);
      }
    } catch (RuntimeException var20) {
      RuntimeException e = var20;
      return Pair.of(ctx, this.handleException(e, httpActionAdapter, webContext));
    }

    return Pair.of(ctx, httpActionAdapter.adapt(action, webContext));
  }

  void setContextRedirectUrl(CallContext ctx) {
    WebContext context = ctx.webContext();
    PlayCookieSessionStore sessionStore = (PlayCookieSessionStore) ctx.sessionStore();

    Optional<Cookie> redirectUrl =
        context.getRequestCookies().stream()
            .filter(cookie -> REDIRECT_URL_COOKIE_NAME.equals(cookie.getName()))
            .findFirst();
    redirectUrl.ifPresent(
        cookie ->
            sessionStore.set(
                context,
                Pac4jConstants.REQUESTED_URL,
                sessionStore
                    .getSerializer()
                    .deserializeFromBytes(
                        uncompressBytes(Base64.getDecoder().decode(cookie.getValue())))));
  }

  Result handleOidcSupportCallback(
      final OperationContext opContext,
      final CallContext ctx,
      final OidcSupportConfigs oidcSupportConfigs,
      final Result result) {

    log.debug("Beginning OIDC Support Callback Handling...");

    ProfileManager profileManager =
        ctx.profileManagerFactory().apply(ctx.webContext(), ctx.sessionStore());

    if (profileManager.isAuthenticated()) {
      // If authenticated, the user should have a profile.
      final Optional<UserProfile> optProfile = profileManager.getProfile();
      if (optProfile.isEmpty()) {
        return internalServerError(
            "Failed to authenticate current user. Cannot find valid identity provider profile in session.");
      }
      final CommonProfile profile = (CommonProfile) optProfile.get();
      log.debug(
          String.format(
              "Found authenticated support user with profile %s",
              profile.getAttributes().toString()));

      // Extract the Username required to log into DataHub.
      final String userName = extractUserNameOrThrow(oidcSupportConfigs, profile);
      final CorpuserUrn corpUserUrn = new CorpuserUrn(userName);

      try {
        // For support staff, always enable JIT provisioning with hardcoded group and role from
        // claim
        log.debug("Support JIT provisioning is enabled. Beginning provisioning process...");
        CorpUserSnapshot extractedUser =
            extractSupportUser(corpUserUrn, profile, oidcSupportConfigs);
        AuthUtils.tryProvisionUser(opContext, extractedUser, systemEntityClient);

        // Determine which group to use - from OIDC claim or default
        String groupName = extractGroupFromClaim(profile, oidcSupportConfigs);
        CorpGroupSnapshot supportGroup = createSupportGroup(oidcSupportConfigs, groupName);

        // Add user to support group
        updateGroupMembership(opContext, corpUserUrn, createSupportGroupMembership(supportGroup));

        // Ensure isSupportUser flag is set to true for this user (even if they already exist)
        // This is important because tryProvisionUser may not update existing users with full
        // aspects
        // Note: We ignore the return value (wasPreviouslySupportUser) for support logins
        AuthUtils.ensureUserSupportFlag(opContext, corpUserUrn, true, systemEntityClient);

        // Extract and assign role from OIDC claim
        String roleFromClaim = extractRoleFromClaim(profile, oidcSupportConfigs);
        String roleToAssign;
        if (roleFromClaim != null && !roleFromClaim.isEmpty()) {
          roleToAssign = roleFromClaim;
        } else {
          // Assign default support role if no role claim is found
          roleToAssign = oidcSupportConfigs.getDefaultRole();
          log.debug("No role found in claim, assigning default support role: {}", roleToAssign);
        }
        AuthUtils.manageUserRole(opContext, corpUserUrn, roleToAssign, true, systemEntityClient);

      } catch (Exception e) {
        log.error(
            "Failed to perform post authentication steps for support user. Redirecting to error page.",
            e);
        return internalServerError(
            String.format(
                "Failed to perform post authentication steps for support user. Error message: %s",
                e.getMessage()));
      }

      log.info("OIDC support callback authentication successful for user: {}", userName);

      // Verify this is a support user before sending notification
      // We just called ensureUserSupportFlag(true) which should have set the flag, but we verify
      // it was set correctly. This defensive check handles potential read-after-write consistency
      // issues where the write succeeded but the read hasn't propagated yet.
      boolean isSupportUser = false;
      try {
        EntityResponse userEntity =
            systemEntityClient.getV2(
                systemOperationContext,
                CORP_USER_ENTITY_NAME,
                corpUserUrn,
                Collections.singleton(CORP_USER_INFO_ASPECT_NAME));
        if (userEntity != null && userEntity.getAspects() != null) {
          EnvelopedAspectMap aspects = userEntity.getAspects();
          EnvelopedAspect corpUserInfoAspect = aspects.get(CORP_USER_INFO_ASPECT_NAME);
          if (corpUserInfoAspect != null) {
            CorpUserInfo userInfo = new CorpUserInfo(corpUserInfoAspect.getValue().data());
            isSupportUser = userInfo.isIsSupportUser() != null && userInfo.isIsSupportUser();
            log.debug("User {} isSupportUser flag: {}", corpUserUrn, isSupportUser);
          }
        }
      } catch (Exception e) {
        log.warn(
            "Failed to verify isSupportUser flag for user: {}, defaulting to true since we're in support callback. Error: {}",
            corpUserUrn,
            e.getMessage());
        // If we can't verify, default to true since we're in the support callback
        // (we just created the user with isSupportUser=true)
        isSupportUser = true;
      }

      // Read ticket ID from cookie if present
      Optional<String> ticketId = readTicketIdFromCookie(ctx);
      if (ticketId.isPresent()) {
        log.debug("Found ticket ID in cookie for support user: {}", ticketId.get());
      }

      // Send support login notification asynchronously if user is a support user
      // (don't block login)
      if (isSupportUser) {
        sendSupportLoginNotification(corpUserUrn.toString(), userName, ticketId, ctx);
      } else {
        log.warn(
            "User {} is not marked as support user in CorpUserInfo, skipping support login notification",
            corpUserUrn);
      }

      // Successfully logged in - Generate GMS login token
      final String accessToken =
          authClient.generateSessionTokenForUser(
              corpUserUrn.getId(), SSO_LOGIN, ticketId.orElse(null));

      // Use session cookies for support users (expire when browser closes)
      Http.Cookie actorCookie =
          createSupportUserActorCookie(
              corpUserUrn.toString(),
              cookieConfigs.getAuthCookieSameSite(),
              cookieConfigs.getAuthCookieSecure());

      Result resultWithCookies =
          result
              .withSession(createSessionMap(corpUserUrn.toString(), accessToken))
              .withCookies(actorCookie);

      // Add ticket ID session cookie if present and non-empty
      if (ticketId.isPresent() && ticketId.get() != null && !ticketId.get().trim().isEmpty()) {
        Http.Cookie ticketCookie =
            createSupportTicketIdSessionCookie(
                ticketId.get(),
                cookieConfigs.getAuthCookieSameSite(),
                cookieConfigs.getAuthCookieSecure());
        return resultWithCookies.withCookies(ticketCookie);
      }

      return resultWithCookies;
    }
    return internalServerError(
        "Failed to authenticate current user. Cannot find valid identity provider profile in session.");
  }

  String extractUserNameOrThrow(
      final OidcSupportConfigs oidcSupportConfigs, final CommonProfile profile) {
    // Ensure that the attribute exists (was returned by IdP)
    if (!profile.containsAttribute(oidcSupportConfigs.getUserNameClaim())) {
      throw new RuntimeException(
          String.format(
              "Failed to resolve user name claim from profile provided by Identity Provider. Missing attribute. Attribute: '%s', Regex: '%s', Profile: %s",
              oidcSupportConfigs.getUserNameClaim(),
              oidcSupportConfigs.getUserNameClaimRegex(),
              profile.getAttributes().toString()));
    }

    final String userNameClaim =
        (String) profile.getAttribute(oidcSupportConfigs.getUserNameClaim());

    final Optional<String> mappedUserName =
        extractRegexGroup(oidcSupportConfigs.getUserNameClaimRegex(), userNameClaim);

    return mappedUserName.orElseThrow(
        () ->
            new RuntimeException(
                String.format(
                    "Failed to extract DataHub username from username claim %s using regex %s. Profile: %s",
                    userNameClaim,
                    oidcSupportConfigs.getUserNameClaimRegex(),
                    profile.getAttributes().toString())));
  }

  /**
   * Attempts to map to an OIDC {@link CommonProfile} (userInfo) to a {@link CorpUserSnapshot} for
   * support staff.
   */
  @VisibleForTesting
  public CorpUserSnapshot extractSupportUser(
      CorpuserUrn urn, CommonProfile profile, OidcSupportConfigs configs) {

    log.debug(
        String.format(
            "Attempting to extract support user from OIDC profile %s",
            profile.getAttributes().toString()));

    // Extracts these based on the default set of OIDC claims
    String firstName = profile.getFirstName();
    String lastName = profile.getFamilyName();
    String email = profile.getEmail();
    URI picture = profile.getPictureUrl();
    String displayName = profile.getDisplayName();
    String fullName =
        (String)
            profile.getAttribute("name"); // Name claim is sometimes provided, including by Google.
    if (fullName == null && firstName != null && lastName != null) {
      fullName = String.format("%s %s", firstName, lastName);
    }

    final CorpUserInfo userInfo = new CorpUserInfo();
    userInfo.setActive(true);
    userInfo.setIsSupportUser(true);
    userInfo.setFirstName(firstName, SetMode.IGNORE_NULL);
    userInfo.setLastName(lastName, SetMode.IGNORE_NULL);
    userInfo.setFullName(fullName, SetMode.IGNORE_NULL);
    userInfo.setEmail(email, SetMode.IGNORE_NULL);
    // If there is a display name, use it. Otherwise fall back to full name.
    userInfo.setDisplayName(
        displayName == null ? userInfo.getFullName() : displayName, SetMode.IGNORE_NULL);

    final CorpUserEditableInfo editableInfo = new CorpUserEditableInfo();
    // For support users, always use configured image or fallback, never OIDC profile picture
    String configuredPictureLink = configs.getUserPictureLink();
    if (configuredPictureLink != null && !configuredPictureLink.isEmpty()) {
      log.debug("Using configured support userPictureLink: {}", configuredPictureLink);
      editableInfo.setPictureLink(new Url(configuredPictureLink));
    } else {
      // Fall back to Acryl dark logo for support users
      String fallbackPicture = OidcSupportConfigs.DEFAULT_SUPPORT_USER_FALLBACK_PICTURE;
      log.debug("Using fallback picture for support user: {}", fallbackPicture);
      editableInfo.setPictureLink(new Url(fallbackPicture));
    }

    final CorpUserSnapshot corpUserSnapshot = new CorpUserSnapshot();
    corpUserSnapshot.setUrn(urn);
    final CorpUserAspectArray aspects = new CorpUserAspectArray();
    aspects.add(CorpUserAspect.create(userInfo));
    aspects.add(CorpUserAspect.create(editableInfo));
    corpUserSnapshot.setAspects(aspects);

    return corpUserSnapshot;
  }

  /**
   * Extracts the group name from the OIDC profile using the configured group claim. Falls back to
   * the default group if no claim is configured or no value is found.
   *
   * @param profile The OIDC user profile
   * @param configs The support OIDC configuration
   * @return The group name to use
   */
  String extractGroupFromClaim(CommonProfile profile, OidcSupportConfigs configs) {
    String groupClaim = configs.getGroupClaim();

    if (groupClaim == null || groupClaim.isEmpty()) {
      // No group claim configured, use default group
      log.debug(
          "No group claim configured, using default group: {}",
          OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME);
      return OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME;
    }

    try {
      Object groupValue = profile.getAttribute(groupClaim);

      if (groupValue != null) {
        String group = groupValue.toString();
        log.debug("Extracted group '{}' from claim '{}'", group, groupClaim);
        return group;
      } else {
        log.debug(
            "No group found in claim '{}', using default group: {}",
            groupClaim,
            OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME);
        return OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME;
      }
    } catch (Exception e) {
      log.warn(
          "Failed to extract group from claim '{}': {}, using default group: {}",
          groupClaim,
          e.getMessage(),
          OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME);
      return OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME;
    }
  }

  /** Creates a support group with the specified name */
  CorpGroupSnapshot createSupportGroup(OidcSupportConfigs configs, String groupName) {
    final CorpGroupInfo corpGroupInfo = new CorpGroupInfo();
    corpGroupInfo.setAdmins(new CorpuserUrnArray());
    corpGroupInfo.setGroups(new CorpGroupUrnArray());
    corpGroupInfo.setMembers(new CorpuserUrnArray());
    corpGroupInfo.setDisplayName(groupName);

    final String urlEncodedGroupName = URLEncoder.encode(groupName, StandardCharsets.UTF_8);
    final CorpGroupUrn groupUrn = new CorpGroupUrn(urlEncodedGroupName);
    final CorpGroupSnapshot corpGroupSnapshot = new CorpGroupSnapshot();
    corpGroupSnapshot.setUrn(groupUrn);
    final CorpGroupAspectArray aspects = new CorpGroupAspectArray();
    aspects.add(CorpGroupAspect.create(corpGroupInfo));
    corpGroupSnapshot.setAspects(aspects);

    return corpGroupSnapshot;
  }

  GroupMembership createSupportGroupMembership(final CorpGroupSnapshot supportGroup) {
    final GroupMembership groupMembershipAspect = new GroupMembership();
    groupMembershipAspect.setGroups(new UrnArray(Arrays.asList(supportGroup.getUrn())));
    return groupMembershipAspect;
  }

  void updateGroupMembership(
      @Nonnull OperationContext opContext, Urn urn, GroupMembership groupMembership) {
    log.debug(String.format("Updating group membership for support user %s", urn));
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(CORP_USER_ENTITY_NAME);
    proposal.setAspectName(GROUP_MEMBERSHIP_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(groupMembership));
    proposal.setChangeType(ChangeType.UPSERT);
    try {
      systemEntityClient.ingestProposal(opContext, proposal);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(
          String.format("Failed to update group membership for support user with urn %s", urn), e);
    }
  }

  Optional<String> extractRegexGroup(final String patternStr, final String target) {
    final Pattern pattern = Pattern.compile(patternStr);
    final Matcher matcher = pattern.matcher(target);
    if (matcher.find()) {
      final String extractedValue = matcher.group();
      return Optional.of(extractedValue);
    }
    return Optional.empty();
  }

  /**
   * Extracts the role from the OIDC profile using the configured role claim.
   *
   * @param profile The OIDC user profile
   * @param configs The support OIDC configuration
   * @return The role value from the claim, or null if not found
   */
  String extractRoleFromClaim(CommonProfile profile, OidcSupportConfigs configs) {
    try {
      String roleClaim = configs.getRoleClaim();
      Object roleValue = profile.getAttribute(roleClaim);

      if (roleValue != null) {
        String role = roleValue.toString();
        log.debug("Extracted role '{}' from claim '{}'", role, roleClaim);
        return role;
      } else {
        log.debug("No role found in claim '{}'", roleClaim);
        return null;
      }
    } catch (Exception e) {
      log.warn(
          "Failed to extract role from claim '{}': {}", configs.getRoleClaim(), e.getMessage());
      return null;
    }
  }

  /**
   * Reads the support ticket ID from the cookie.
   *
   * @param ctx The call context containing cookies
   * @return Optional ticket ID if present in cookie
   */
  private Optional<String> readTicketIdFromCookie(CallContext ctx) {
    try {
      WebContext webContext = ctx.webContext();
      Optional<Cookie> cookie =
          webContext.getRequestCookies().stream()
              .filter(c -> AuthUtils.SUPPORT_TICKET_ID_COOKIE_NAME.equals(c.getName()))
              .findFirst();
      if (cookie.isPresent()
          && cookie.get().getValue() != null
          && !cookie.get().getValue().isEmpty()) {
        // URL-decode the ticket ID since it was encoded when stored in the cookie
        String decodedTicketId = URLDecoder.decode(cookie.get().getValue(), StandardCharsets.UTF_8);
        return Optional.of(decodedTicketId);
      }
    } catch (Exception e) {
      log.warn("Failed to read ticket ID from cookie: {}", e.getMessage());
    }
    return Optional.empty();
  }

  /**
   * Sends a support login notification to the integrations service asynchronously.
   *
   * @param actorUrn The URN of the user who logged in
   * @param actorName The display name of the user
   * @param ticketId Optional ticket ID if present
   * @param ctx The call context for extracting additional info (e.g., IP address)
   */
  private void sendSupportLoginNotification(
      String actorUrn, String actorName, Optional<String> ticketId, CallContext ctx) {
    CompletableFuture.runAsync(
        () -> {
          try {
            // Build notification request
            NotificationRequest notificationRequest = new NotificationRequest();
            NotificationMessage message = new NotificationMessage();
            message.setTemplate(NotificationTemplateType.SUPPORT_LOGIN);

            // Build parameters map
            StringMap parameters = new StringMap();
            parameters.put("actorUrn", actorUrn);
            parameters.put("actorName", actorName);
            // Convert epoch milliseconds to ISO 8601 format in UTC timezone
            String isoTimestamp =
                Instant.ofEpochMilli(System.currentTimeMillis())
                    .atZone(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ISO_INSTANT);
            parameters.put("timestamp", isoTimestamp);

            if (ticketId.isPresent()) {
              parameters.put("supportTicketId", ticketId.get());
            }

            // Extract source IP if available
            try {
              WebContext webContext = ctx.webContext();
              String sourceIP = webContext.getRequestHeader("X-Forwarded-For").orElse(null);
              if (sourceIP == null || sourceIP.isEmpty()) {
                sourceIP = webContext.getRemoteAddr();
              }
              if (sourceIP != null && !sourceIP.isEmpty()) {
                // Take first IP if multiple (X-Forwarded-For can contain multiple IPs)
                String ip = sourceIP.split(",")[0].trim();
                parameters.put("sourceIP", ip);
              }
            } catch (Exception e) {
              log.debug("Failed to extract source IP: {}", e.getMessage());
            }

            message.setParameters(parameters);
            notificationRequest.setMessage(message);

            // Recipients are handled by the Python service using SUPPORT_LOGIN_EMAIL_RECIPIENTS
            // So we can leave recipients empty or set empty array
            notificationRequest.setRecipients(
                new com.linkedin.event.notification.NotificationRecipientArray());

            // Get integrations service config
            String integrationsServiceHost =
                ConfigUtil.getString(
                    config,
                    AcrylConstants.INTEGRATIONS_SERVICE_HOST_CONFIG_PATH,
                    AcrylConstants.DEFAULT_INTEGRATIONS_SERVICE_HOST);
            int integrationsServicePort =
                ConfigUtil.getInt(
                    config,
                    AcrylConstants.INTEGRATIONS_SERVICE_PORT_CONFIG_PATH,
                    AcrylConstants.DEFAULT_INTEGRATIONS_SERVICE_PORT);
            boolean integrationsServiceUseSsl =
                ConfigUtil.getBoolean(
                    config,
                    AcrylConstants.INTEGRATIONS_SERVICE_USE_SSL_CONFIG_PATH,
                    AcrylConstants.DEFAULT_INTEGRATIONS_SERVICE_USE_SSL);

            String protocol = integrationsServiceUseSsl ? "https" : "http";
            String url =
                String.format(
                    "%s://%s:%d/private/notifications/send",
                    protocol, integrationsServiceHost, integrationsServicePort);

            // Convert notification request to JSON
            String jsonBody = RecordUtils.toJsonString(notificationRequest);

            // Build HTTP request
            HttpRequest httpRequest =
                HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .timeout(Duration.ofSeconds(30))
                    .build();

            // Send request asynchronously
            httpClient
                .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                .thenAccept(
                    response -> {
                      if (response.statusCode() == 200 || response.statusCode() == 202) {
                        log.info(
                            "Successfully sent support login notification for user: {}", actorUrn);
                      } else {
                        log.warn(
                            "Failed to send support login notification. Status: {}, Body: {}",
                            response.statusCode(),
                            response.body());
                      }
                    })
                .exceptionally(
                    throwable -> {
                      log.error(
                          "Exception while sending support login notification for user: {}",
                          actorUrn,
                          throwable);
                      return null;
                    });

          } catch (Exception e) {
            log.error("Failed to send support login notification for user: {}", actorUrn, e);
          }
        });
  }
}
