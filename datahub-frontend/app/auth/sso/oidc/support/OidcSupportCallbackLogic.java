package auth.sso.oidc.support;

import static auth.AuthUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.pac4j.play.store.PlayCookieSessionStore.*;
import static play.mvc.Results.internalServerError;
import static utils.FrontendConstants.SSO_LOGIN;

import auth.AuthUtils;
import auth.CookieConfigs;
import auth.sso.SsoSupportManager;
import auth.sso.oidc.OidcResponseErrorHandler;
import client.AuthServiceClient;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataHubRoleUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.RoleMembership;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
import play.mvc.Result;

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

  public OidcSupportCallbackLogic(
      final SsoSupportManager ssoSupportManager,
      final OperationContext systemOperationContext,
      final SystemEntityClient entityClient,
      final AuthServiceClient authClient,
      final CookieConfigs cookieConfigs) {
    this.ssoSupportManager = ssoSupportManager;
    this.systemOperationContext = systemOperationContext;
    systemEntityClient = entityClient;
    this.authClient = authClient;
    this.cookieConfigs = cookieConfigs;
  }

  @Override
  public Object perform(
      Config config,
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

  private void setContextRedirectUrl(CallContext ctx) {
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

  private Result handleOidcSupportCallback(
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

        // Extract and assign role from OIDC claim
        String roleFromClaim = extractRoleFromClaim(profile, oidcSupportConfigs);
        if (roleFromClaim != null && !roleFromClaim.isEmpty()) {
          assignRoleToUser(opContext, corpUserUrn, roleFromClaim);
        } else {
          // Assign default support role if no role claim is found
          String defaultRole = oidcSupportConfigs.getDefaultRole();
          log.debug("No role found in claim, assigning default support role: {}", defaultRole);
          assignRoleToUser(opContext, corpUserUrn, defaultRole);
        }

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

      // Successfully logged in - Generate GMS login token
      final String accessToken =
          authClient.generateSessionTokenForUser(corpUserUrn.getId(), SSO_LOGIN);
      return result
          .withSession(createSessionMap(corpUserUrn.toString(), accessToken))
          .withCookies(
              createActorCookie(
                  corpUserUrn.toString(),
                  cookieConfigs.getTtlInHours(),
                  cookieConfigs.getAuthCookieSameSite(),
                  cookieConfigs.getAuthCookieSecure()));
    }
    return internalServerError(
        "Failed to authenticate current user. Cannot find valid identity provider profile in session.");
  }

  private String extractUserNameOrThrow(
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
  private String extractGroupFromClaim(CommonProfile profile, OidcSupportConfigs configs) {
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
  private CorpGroupSnapshot createSupportGroup(OidcSupportConfigs configs, String groupName) {
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

  private GroupMembership createSupportGroupMembership(final CorpGroupSnapshot supportGroup) {
    final GroupMembership groupMembershipAspect = new GroupMembership();
    groupMembershipAspect.setGroups(new UrnArray(Arrays.asList(supportGroup.getUrn())));
    return groupMembershipAspect;
  }

  private void updateGroupMembership(
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

  private Optional<String> extractRegexGroup(final String patternStr, final String target) {
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
  private String extractRoleFromClaim(CommonProfile profile, OidcSupportConfigs configs) {
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
   * Assigns a role to a user by creating a RoleMembership aspect.
   *
   * @param opContext The operation context
   * @param userUrn The user URN
   * @param roleName The role name to assign
   */
  private void assignRoleToUser(OperationContext opContext, CorpuserUrn userUrn, String roleName) {
    try {
      // Create the role URN
      DataHubRoleUrn roleUrn = new DataHubRoleUrn(roleName);

      // Create RoleMembership aspect
      RoleMembership roleMembership = new RoleMembership();
      roleMembership.setRoles(new UrnArray(roleUrn));

      // Create MetadataChangeProposal for role assignment
      final MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(userUrn);
      proposal.setEntityType(CORP_USER_ENTITY_NAME);
      proposal.setAspectName(ROLE_MEMBERSHIP_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(roleMembership));
      proposal.setChangeType(ChangeType.UPSERT);

      // Ingest the role membership proposal
      systemEntityClient.ingestProposal(opContext, proposal);

      log.info("Successfully assigned role '{}' to user '{}'", roleName, userUrn);
    } catch (Exception e) {
      log.error(
          "Failed to assign role '{}' to user '{}': {}", roleName, userUrn, e.getMessage(), e);
      // Don't throw the exception to avoid breaking the login flow
    }
  }
}
