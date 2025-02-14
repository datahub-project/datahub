package auth.sso.oidc;

import static auth.AuthUtils.*;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static org.pac4j.play.store.PlayCookieSessionStore.*;
import static play.mvc.Results.internalServerError;

import auth.CookieConfigs;
import auth.sso.SsoManager;
import client.AuthServiceClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.aspect.CorpGroupAspectArray;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.mvc.Result;

/**
 * This class contains the logic that is executed when an OpenID Connect Identity Provider redirects
 * back to D DataHub after an authentication attempt.
 *
 * <p>On receiving a user profile from the IdP (using /userInfo endpoint), we attempt to extract
 * basic information about the user including their name, email, groups, & more. If just-in-time
 * provisioning is enabled, we also attempt to create a DataHub User ({@link CorpUserSnapshot}) for
 * the user, along with any Groups ({@link CorpGroupSnapshot}) that can be extracted, only doing so
 * if the user does not already exist.
 */
@Slf4j
public class OidcCallbackLogic extends DefaultCallbackLogic {
  private static final Logger LOGGER = LoggerFactory.getLogger(OidcCallbackLogic.class);

  private final SsoManager ssoManager;
  private final SystemEntityClient systemEntityClient;
  private final OperationContext systemOperationContext;
  private final AuthServiceClient authClient;
  private final CookieConfigs cookieConfigs;

  public OidcCallbackLogic(
      final SsoManager ssoManager,
      final OperationContext systemOperationContext,
      final SystemEntityClient entityClient,
      final AuthServiceClient authClient,
      final CookieConfigs cookieConfigs) {
    this.ssoManager = ssoManager;
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

    // By this point, we know that OIDC is the enabled provider.
    final OidcConfigs oidcConfigs = (OidcConfigs) ssoManager.getSsoProvider().configs();
    return handleOidcCallback(systemOperationContext, ctx, oidcConfigs, result);
  }

  /** Overriding this to be able to intercept the CallContext being created */
  private Pair<CallContext, Object> superPerform(
      Config config,
      String inputDefaultUrl,
      Boolean inputRenewSession,
      String defaultClient,
      FrameworkParameters parameters) {
    LOGGER.debug("=== CALLBACK ===");
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
      LOGGER.debug("foundClient: {}", foundClient);
      CommonHelper.assertNotNull("foundClient", foundClient);
      Credentials credentials = (Credentials) foundClient.getCredentials(ctx).orElse(null);
      LOGGER.debug("extracted credentials: {}", credentials);
      credentials = (Credentials) foundClient.validateCredentials(ctx, credentials).orElse(null);
      LOGGER.debug("validated credentials: {}", credentials);
      if (credentials != null && !credentials.isForAuthentication()) {
        action = foundClient.processLogout(ctx, credentials);
      } else {
        if (credentials != null) {
          Optional<UserProfile> optProfile = foundClient.getUserProfile(ctx, credentials);
          LOGGER.debug("optProfile: {}", optProfile);
          if (optProfile.isPresent()) {
            UserProfile profile = (UserProfile) optProfile.get();
            Boolean saveProfileInSession =
                ((BaseClient) foundClient).getSaveProfileInSession(webContext, profile);
            boolean multiProfile = ((BaseClient) foundClient).isMultiProfile(webContext, profile);
            LOGGER.debug(
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

  private Result handleOidcCallback(
      final OperationContext opContext,
      final CallContext ctx,
      final OidcConfigs oidcConfigs,
      final Result result) {

    log.debug("Beginning OIDC Callback Handling...");

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
              "Found authenticated user with profile %s", profile.getAttributes().toString()));

      // Extract the User name required to log into DataHub.
      final String userName = extractUserNameOrThrow(oidcConfigs, profile);
      final CorpuserUrn corpUserUrn = new CorpuserUrn(userName);

      try {
        // If just-in-time User Provisioning is enabled, try to create the DataHub user if it does
        // not exist.
        if (oidcConfigs.isJitProvisioningEnabled()) {
          log.debug("Just-in-time provisioning is enabled. Beginning provisioning process...");
          CorpUserSnapshot extractedUser = extractUser(corpUserUrn, profile);
          tryProvisionUser(opContext, extractedUser);
          if (oidcConfigs.isExtractGroupsEnabled()) {
            // Extract groups & provision them.
            List<CorpGroupSnapshot> extractedGroups = extractGroups(profile);
            tryProvisionGroups(opContext, extractedGroups);
            // Add users to groups on DataHub. Note that this clears existing group membership for a
            // user if it already exists.
            updateGroupMembership(opContext, corpUserUrn, createGroupMembership(extractedGroups));
          }
        } else if (oidcConfigs.isPreProvisioningRequired()) {
          // We should only allow logins for user accounts that have been pre-provisioned
          log.debug("Pre Provisioning is required. Beginning validation of extracted user...");
          verifyPreProvisionedUser(opContext, corpUserUrn);
        }
        // Update user status to active on login.
        // If we want to prevent certain users from logging in, here's where we'll want to do it.
        setUserStatus(
            opContext,
            corpUserUrn,
            new CorpUserStatus()
                .setStatus(Constants.CORP_USER_STATUS_ACTIVE)
                .setLastModified(
                    new AuditStamp()
                        .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                        .setTime(System.currentTimeMillis())));
      } catch (Exception e) {
        log.error("Failed to perform post authentication steps. Redirecting to error page.", e);
        return internalServerError(
            String.format(
                "Failed to perform post authentication steps. Error message: %s", e.getMessage()));
      }

      log.info("OIDC callback authentication successful for user: {}", userName);

      // Successfully logged in - Generate GMS login token
      final String accessToken = authClient.generateSessionTokenForUser(corpUserUrn.getId());
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
      final OidcConfigs oidcConfigs, final CommonProfile profile) {
    // Ensure that the attribute exists (was returned by IdP)
    if (!profile.containsAttribute(oidcConfigs.getUserNameClaim())) {
      throw new RuntimeException(
          String.format(
              "Failed to resolve user name claim from profile provided by Identity Provider. Missing attribute. Attribute: '%s', Regex: '%s', Profile: %s",
              oidcConfigs.getUserNameClaim(),
              oidcConfigs.getUserNameClaimRegex(),
              profile.getAttributes().toString()));
    }

    final String userNameClaim = (String) profile.getAttribute(oidcConfigs.getUserNameClaim());

    final Optional<String> mappedUserName =
        extractRegexGroup(oidcConfigs.getUserNameClaimRegex(), userNameClaim);

    return mappedUserName.orElseThrow(
        () ->
            new RuntimeException(
                String.format(
                    "Failed to extract DataHub username from username claim %s using regex %s. Profile: %s",
                    userNameClaim,
                    oidcConfigs.getUserNameClaimRegex(),
                    profile.getAttributes().toString())));
  }

  /** Attempts to map to an OIDC {@link CommonProfile} (userInfo) to a {@link CorpUserSnapshot}. */
  private CorpUserSnapshot extractUser(CorpuserUrn urn, CommonProfile profile) {

    log.debug(
        String.format(
            "Attempting to extract user from OIDC profile %s", profile.getAttributes().toString()));

    // Extracts these based on the default set of OIDC claims, described here:
    // https://developer.okta.com/blog/2017/07/25/oidc-primer-part-1
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

    // TODO: Support custom claims mapping. (e.g. department, title, etc)

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
    try {
      if (picture != null) {
        editableInfo.setPictureLink(new Url(picture.toURL().toString()));
      }
    } catch (MalformedURLException e) {
      log.error("Failed to extract User Profile URL skipping.", e);
    }

    final CorpUserSnapshot corpUserSnapshot = new CorpUserSnapshot();
    corpUserSnapshot.setUrn(urn);
    final CorpUserAspectArray aspects = new CorpUserAspectArray();
    aspects.add(CorpUserAspect.create(userInfo));
    aspects.add(CorpUserAspect.create(editableInfo));
    corpUserSnapshot.setAspects(aspects);

    return corpUserSnapshot;
  }

  public static Collection<String> getGroupNames(
      CommonProfile profile, Object groupAttribute, String groupsClaimName) {
    Collection<String> groupNames = Collections.emptyList();
    try {
      if (groupAttribute instanceof Collection) {
        // List of group names
        groupNames = (Collection<String>) profile.getAttribute(groupsClaimName, Collection.class);
      } else if (groupAttribute instanceof String) {
        String groupString = (String) groupAttribute;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
          // Json list of group names
          groupNames = objectMapper.readValue(groupString, new TypeReference<List<String>>() {});
        } catch (Exception e) {
          groupNames = Arrays.asList(groupString.split(","));
        }
      }
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to parse group names: Expected to find a list of strings for attribute with name %s, found %s",
              groupsClaimName, profile.getAttribute(groupsClaimName).getClass()));
    }
    return groupNames;
  }

  private List<CorpGroupSnapshot> extractGroups(CommonProfile profile) {

    log.debug(
        String.format(
            "Attempting to extract groups from OIDC profile %s",
            profile.getAttributes().toString()));
    final OidcConfigs configs = (OidcConfigs) ssoManager.getSsoProvider().configs();

    // First, attempt to extract a list of groups from the profile, using the group name attribute
    // config.
    final List<CorpGroupSnapshot> extractedGroups = new ArrayList<>();
    final List<String> groupsClaimNames =
        new ArrayList<String>(Arrays.asList(configs.getGroupsClaimName().split(",")))
            .stream().map(String::trim).collect(Collectors.toList());

    for (final String groupsClaimName : groupsClaimNames) {

      if (profile.containsAttribute(groupsClaimName)) {
        try {
          final List<CorpGroupSnapshot> groupSnapshots = new ArrayList<>();
          Collection<String> groupNames =
              getGroupNames(profile, profile.getAttribute(groupsClaimName), groupsClaimName);

          for (String groupName : groupNames) {
            // Create a basic CorpGroupSnapshot from the information.
            try {

              final CorpGroupInfo corpGroupInfo = new CorpGroupInfo();
              corpGroupInfo.setAdmins(new CorpuserUrnArray());
              corpGroupInfo.setGroups(new CorpGroupUrnArray());
              corpGroupInfo.setMembers(new CorpuserUrnArray());
              corpGroupInfo.setEmail("");
              corpGroupInfo.setDisplayName(groupName);

              // To deal with the possibility of spaces, we url encode the URN group name.
              final String urlEncodedGroupName =
                  URLEncoder.encode(groupName, StandardCharsets.UTF_8.toString());
              final CorpGroupUrn groupUrn = new CorpGroupUrn(urlEncodedGroupName);
              final CorpGroupSnapshot corpGroupSnapshot = new CorpGroupSnapshot();
              corpGroupSnapshot.setUrn(groupUrn);
              final CorpGroupAspectArray aspects = new CorpGroupAspectArray();
              aspects.add(CorpGroupAspect.create(corpGroupInfo));
              corpGroupSnapshot.setAspects(aspects);
              groupSnapshots.add(corpGroupSnapshot);
            } catch (UnsupportedEncodingException ex) {
              log.error(
                  String.format(
                      "Failed to URL encoded extracted group name %s. Skipping", groupName));
            }
          }
          if (groupSnapshots.isEmpty()) {
            log.warn(
                String.format(
                    "Failed to extract groups: No OIDC claim with name %s found", groupsClaimName));
          } else {
            extractedGroups.addAll(groupSnapshots);
          }
        } catch (Exception e) {
          log.error(
              String.format(
                  "Failed to extract groups: Expected to find a list of strings for attribute with name %s, found %s",
                  groupsClaimName, profile.getAttribute(groupsClaimName).getClass()));
        }
      }
    }
    return extractedGroups;
  }

  private GroupMembership createGroupMembership(final List<CorpGroupSnapshot> extractedGroups) {
    final GroupMembership groupMembershipAspect = new GroupMembership();
    groupMembershipAspect.setGroups(
        new UrnArray(
            extractedGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList())));
    return groupMembershipAspect;
  }

  private void tryProvisionUser(
      @Nonnull OperationContext opContext, CorpUserSnapshot corpUserSnapshot) {

    log.debug(String.format("Attempting to provision user with urn %s", corpUserSnapshot.getUrn()));

    // 1. Check if this user already exists.
    try {
      final Entity corpUser = systemEntityClient.get(opContext, corpUserSnapshot.getUrn());
      final CorpUserSnapshot existingCorpUserSnapshot = corpUser.getValue().getCorpUserSnapshot();

      log.debug(String.format("Fetched GMS user with urn %s", corpUserSnapshot.getUrn()));

      // If we find more than the key aspect, then the entity "exists".
      if (existingCorpUserSnapshot.getAspects().size() <= 1) {
        log.debug(
            String.format(
                "Extracted user that does not yet exist %s. Provisioning...",
                corpUserSnapshot.getUrn()));
        // 2. The user does not exist. Provision them.
        final Entity newEntity = new Entity();
        newEntity.setValue(Snapshot.create(corpUserSnapshot));
        systemEntityClient.update(opContext, newEntity);
        log.debug(String.format("Successfully provisioned user %s", corpUserSnapshot.getUrn()));
      }
      log.debug(
          String.format(
              "User %s already exists. Skipping provisioning", corpUserSnapshot.getUrn()));
      // Otherwise, the user exists. Skip provisioning.
    } catch (RemoteInvocationException e) {
      // Failing provisioning is something worth throwing about.
      throw new RuntimeException(
          String.format("Failed to provision user with urn %s.", corpUserSnapshot.getUrn()), e);
    }
  }

  private void tryProvisionGroups(
      @Nonnull OperationContext opContext, List<CorpGroupSnapshot> corpGroups) {

    log.debug(
        String.format(
            "Attempting to provision groups with urns %s",
            corpGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList())));

    // 1. Check if this user already exists.
    try {
      final Set<Urn> urnsToFetch =
          corpGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toSet());
      final Map<Urn, Entity> existingGroups = systemEntityClient.batchGet(opContext, urnsToFetch);

      log.debug(String.format("Fetched GMS groups with urns %s", existingGroups.keySet()));

      final List<CorpGroupSnapshot> groupsToCreate = new ArrayList<>();
      for (CorpGroupSnapshot extractedGroup : corpGroups) {
        if (existingGroups.containsKey(extractedGroup.getUrn())) {

          final Entity groupEntity = existingGroups.get(extractedGroup.getUrn());
          final CorpGroupSnapshot corpGroupSnapshot = groupEntity.getValue().getCorpGroupSnapshot();

          // If more than the key aspect exists, then the group already "exists".
          if (corpGroupSnapshot.getAspects().size() <= 1) {
            log.debug(
                String.format(
                    "Extracted group that does not yet exist %s. Provisioning...",
                    corpGroupSnapshot.getUrn()));
            groupsToCreate.add(extractedGroup);
          }
          log.debug(
              String.format(
                  "Group %s already exists. Skipping provisioning", corpGroupSnapshot.getUrn()));
        } else {
          // Should not occur until we stop returning default Key aspects for unrecognized entities.
          log.debug(
              String.format(
                  "Extracted group that does not yet exist %s. Provisioning...",
                  extractedGroup.getUrn()));
          groupsToCreate.add(extractedGroup);
        }
      }

      List<Urn> groupsToCreateUrns =
          groupsToCreate.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList());

      log.debug(String.format("Provisioning groups with urns %s", groupsToCreateUrns));

      // Now batch create all entities identified to create.
      systemEntityClient.batchUpdate(
          opContext,
          groupsToCreate.stream()
              .map(groupSnapshot -> new Entity().setValue(Snapshot.create(groupSnapshot)))
              .collect(Collectors.toSet()));

      log.debug(String.format("Successfully provisioned groups with urns %s", groupsToCreateUrns));
    } catch (RemoteInvocationException e) {
      // Failing provisioning is something worth throwing about.
      throw new RuntimeException(
          String.format(
              "Failed to provision groups with urns %s.",
              corpGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  private void updateGroupMembership(
      @Nonnull OperationContext opContext, Urn urn, GroupMembership groupMembership) {
    log.debug(String.format("Updating group membership for user %s", urn));
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
          String.format("Failed to update group membership for user with urn %s", urn), e);
    }
  }

  private void verifyPreProvisionedUser(@Nonnull OperationContext opContext, CorpuserUrn urn) {
    // Validate that the user exists in the system (there is more than just a key aspect for them,
    // as of today).
    try {
      final Entity corpUser = systemEntityClient.get(opContext, urn);

      log.debug(String.format("Fetched GMS user with urn %s", urn));

      // If we find more than the key aspect, then the entity "exists".
      if (corpUser.getValue().getCorpUserSnapshot().getAspects().size() <= 1) {
        log.debug(
            String.format(
                "Found user that does not yet exist %s. Invalid login attempt. Throwing...", urn));
        throw new RuntimeException(
            String.format(
                "User with urn %s has not yet been provisioned in DataHub. "
                    + "Please contact your DataHub admin to provision an account.",
                urn));
      }
      // Otherwise, the user exists.
    } catch (RemoteInvocationException e) {
      // Failing validation is something worth throwing about.
      throw new RuntimeException(String.format("Failed to validate user with urn %s.", urn), e);
    }
  }

  private void setUserStatus(
      @Nonnull OperationContext opContext, final Urn urn, final CorpUserStatus newStatus)
      throws Exception {
    // Update status aspect to be active.
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
    proposal.setAspectName(Constants.CORP_USER_STATUS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newStatus));
    proposal.setChangeType(ChangeType.UPSERT);
    systemEntityClient.ingestProposal(opContext, proposal);
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
}
