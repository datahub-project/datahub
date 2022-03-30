package auth.sso.oidc;

import client.AuthServiceClient;
import com.datahub.authentication.Authentication;
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
import com.linkedin.entity.client.EntityClient;
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
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.core.config.Config;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.http.adapter.HttpActionAdapter;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;
import play.mvc.Result;
import auth.sso.SsoManager;

import static com.linkedin.metadata.Constants.*;
import static play.mvc.Results.*;
import static auth.AuthUtils.*;


/**
 * This class contains the logic that is executed when an OpenID Connect Identity Provider redirects back to D
 * DataHub after an authentication attempt.
 *
 * On receiving a user profile from the IdP (using /userInfo endpoint), we attempt to extract
 * basic information about the user including their name, email, groups, & more. If just-in-time provisioning
 * is enabled, we also attempt to create a DataHub User ({@link CorpUserSnapshot}) for the user, along with any Groups
 * ({@link CorpGroupSnapshot}) that can be extracted, only doing so if the user does not already exist.
 */
@Slf4j
public class OidcCallbackLogic extends DefaultCallbackLogic<Result, PlayWebContext> {

  private final SsoManager _ssoManager;
  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;
  private final AuthServiceClient _authClient;

  public OidcCallbackLogic(final SsoManager ssoManager, final Authentication systemAuthentication,
      final EntityClient entityClient, final AuthServiceClient authClient) {
    _ssoManager = ssoManager;
    _systemAuthentication = systemAuthentication;
    _entityClient = entityClient;
    _authClient = authClient;
  }

  @Override
  public Result perform(PlayWebContext context, Config config,
      HttpActionAdapter<Result, PlayWebContext> httpActionAdapter, String defaultUrl, Boolean saveInSession,
      Boolean multiProfile, Boolean renewSession, String defaultClient) {
    final Result result =
        super.perform(context, config, httpActionAdapter, defaultUrl, saveInSession, multiProfile, renewSession,
            defaultClient);

    // Handle OIDC authentication errors.
    if (OidcResponseErrorHandler.isError(context)) {
      return OidcResponseErrorHandler.handleError(context);
    }

    // By this point, we know that OIDC is the enabled provider.
    final OidcConfigs oidcConfigs = (OidcConfigs) _ssoManager.getSsoProvider().configs();
    return handleOidcCallback(oidcConfigs, result, context, getProfileManager(context, config));
  }

  private Result handleOidcCallback(final OidcConfigs oidcConfigs, final Result result, final PlayWebContext context,
      final ProfileManager<CommonProfile> profileManager) {

    log.debug("Beginning OIDC Callback Handling...");

    if (profileManager.isAuthenticated()) {
      // If authenticated, the user should have a profile.
      final CommonProfile profile = profileManager.get(true).get();
      log.debug(String.format("Found authenticated user with profile %s", profile.getAttributes().toString()));

      // Extract the User name required to log into DataHub.
      final String userName = extractUserNameOrThrow(oidcConfigs, profile);
      final CorpuserUrn corpUserUrn = new CorpuserUrn(userName);

      try {
        // If just-in-time User Provisioning is enabled, try to create the DataHub user if it does not exist.
        if (oidcConfigs.isJitProvisioningEnabled()) {
          log.debug("Just-in-time provisioning is enabled. Beginning provisioning process...");
          CorpUserSnapshot extractedUser = extractUser(corpUserUrn, profile);
          tryProvisionUser(extractedUser);
          if (oidcConfigs.isExtractGroupsEnabled()) {
            // Extract groups & provision them.
            List<CorpGroupSnapshot> extractedGroups = extractGroups(profile);
            tryProvisionGroups(extractedGroups);
            // Add users to groups on DataHub. Note that this clears existing group membership for a user if it already exists.
            updateGroupMembership(corpUserUrn, createGroupMembership(extractedGroups));
          }
        } else if (oidcConfigs.isPreProvisioningRequired()) {
          // We should only allow logins for user accounts that have been pre-provisioned
          log.debug("Pre Provisioning is required. Beginning validation of extracted user...");
          verifyPreProvisionedUser(corpUserUrn);
        }
        // Update user status to active on login.
        // If we want to prevent certain users from logging in, here's where we'll want to do it.
        setUserStatus(corpUserUrn, new CorpUserStatus().setStatus(Constants.CORP_USER_STATUS_ACTIVE)
            .setLastModified(new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis())));
      } catch (Exception e) {
        log.error("Failed to perform post authentication steps. Redirecting to error page.", e);
        return internalServerError(
            String.format("Failed to perform post authentication steps. Error message: %s", e.getMessage()));
      }

      // Successfully logged in - Generate GMS login token
      final String accessToken = _authClient.generateSessionTokenForUser(corpUserUrn.getId());
      context.getJavaSession().put(ACCESS_TOKEN, accessToken);
      context.getJavaSession().put(ACTOR, corpUserUrn.toString());
      return result.withCookies(createActorCookie(corpUserUrn.toString(), oidcConfigs.getSessionTtlInHours()));
    }
    return internalServerError(
        "Failed to authenticate current user. Cannot find valid identity provider profile in session.");
  }

  private String extractUserNameOrThrow(final OidcConfigs oidcConfigs, final CommonProfile profile) {
    // Ensure that the attribute exists (was returned by IdP)
    if (!profile.containsAttribute(oidcConfigs.getUserNameClaim())) {
      throw new RuntimeException(String.format(
          "Failed to resolve user name claim from profile provided by Identity Provider. Missing attribute. Attribute: '%s', Regex: '%s', Profile: %s",
          oidcConfigs.getUserNameClaim(), oidcConfigs.getUserNameClaimRegex(), profile.getAttributes().toString()));
    }

    final String userNameClaim = (String) profile.getAttribute(oidcConfigs.getUserNameClaim());

    final Optional<String> mappedUserName = extractRegexGroup(oidcConfigs.getUserNameClaimRegex(), userNameClaim);

    return mappedUserName.orElseThrow(() -> new RuntimeException(
        String.format("Failed to extract DataHub username from username claim %s using regex %s. Profile: %s",
            userNameClaim, oidcConfigs.getUserNameClaimRegex(), profile.getAttributes().toString())));
  }

  /**
   * Attempts to map to an OIDC {@link CommonProfile} (userInfo) to a {@link CorpUserSnapshot}.
   */
  private CorpUserSnapshot extractUser(CorpuserUrn urn, CommonProfile profile) {

    log.debug(String.format("Attempting to extract user from OIDC profile %s", profile.getAttributes().toString()));

    // Extracts these based on the default set of OIDC claims, described here:
    // https://developer.okta.com/blog/2017/07/25/oidc-primer-part-1
    String firstName = profile.getFirstName();
    String lastName = profile.getFamilyName();
    String email = profile.getEmail();
    URI picture = profile.getPictureUrl();
    String displayName = profile.getDisplayName();
    String fullName = (String) profile.getAttribute("name"); // Name claim is sometimes provided, including by Google.
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
    userInfo.setDisplayName(displayName == null ? userInfo.getFullName() : displayName, SetMode.IGNORE_NULL);

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

  private List<CorpGroupSnapshot> extractGroups(CommonProfile profile) {

    log.debug(String.format("Attempting to extract groups from OIDC profile %s", profile.getAttributes().toString()));
    final OidcConfigs configs = (OidcConfigs) _ssoManager.getSsoProvider().configs();

    // First, attempt to extract a list of groups from the profile, using the group name attribute config.
    final List<CorpGroupSnapshot> extractedGroups = new ArrayList<>();
    final List<String> groupsClaimNames =
        new ArrayList<String>(Arrays.asList(configs.getGroupsClaimName().split(","))).stream()
            .map(String::trim)
            .collect(Collectors.toList());

    for (final String groupsClaimName : groupsClaimNames) {

      if (profile.containsAttribute(groupsClaimName)) {
        try {
          final List<CorpGroupSnapshot> groupSnapshots = new ArrayList<>();
          final Collection<String> groupNames;
          final Object groupAttribute = profile.getAttribute(groupsClaimName);
          if (groupAttribute instanceof Collection) {
            // List of group names
            groupNames = (Collection<String>) profile.getAttribute(groupsClaimName, Collection.class);
          } else if (groupAttribute instanceof String) {
            // Single group name
            groupNames = Collections.singleton(profile.getAttribute(groupsClaimName, String.class));
          } else {
            log.error(
                String.format("Fail to parse OIDC group claim with name %s. Unknown type %s provided.", groupsClaimName,
                    groupAttribute.getClass()));
            // Skip over group attribute. Do not throw.
            groupNames = Collections.emptyList();
          }

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
              final String urlEncodedGroupName = URLEncoder.encode(groupName, StandardCharsets.UTF_8.toString());
              final CorpGroupUrn groupUrn = new CorpGroupUrn(urlEncodedGroupName);
              final CorpGroupSnapshot corpGroupSnapshot = new CorpGroupSnapshot();
              corpGroupSnapshot.setUrn(groupUrn);
              final CorpGroupAspectArray aspects = new CorpGroupAspectArray();
              aspects.add(CorpGroupAspect.create(corpGroupInfo));
              corpGroupSnapshot.setAspects(aspects);
              groupSnapshots.add(corpGroupSnapshot);
            } catch (UnsupportedEncodingException ex) {
              log.error(String.format("Failed to URL encoded extracted group name %s. Skipping", groupName));
            }
          }
          if (groupSnapshots.isEmpty()) {
            log.warn(String.format("Failed to extract groups: No OIDC claim with name %s found", groupsClaimName));
          } else {
            extractedGroups.addAll(groupSnapshots);
          }
        } catch (Exception e) {
          log.error(String.format(
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
        new UrnArray(extractedGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList())));
    return groupMembershipAspect;
  }

  private void tryProvisionUser(CorpUserSnapshot corpUserSnapshot) {

    log.debug(String.format("Attempting to provision user with urn %s", corpUserSnapshot.getUrn()));

    // 1. Check if this user already exists.
    try {
      final Entity corpUser = _entityClient.get(corpUserSnapshot.getUrn(), _systemAuthentication);
      final CorpUserSnapshot existingCorpUserSnapshot = corpUser.getValue().getCorpUserSnapshot();

      log.debug(String.format("Fetched GMS user with urn %s", corpUserSnapshot.getUrn()));

      // If we find more than the key aspect, then the entity "exists".
      if (existingCorpUserSnapshot.getAspects().size() <= 1) {
        log.debug(
            String.format("Extracted user that does not yet exist %s. Provisioning...", corpUserSnapshot.getUrn()));
        // 2. The user does not exist. Provision them.
        final Entity newEntity = new Entity();
        newEntity.setValue(Snapshot.create(corpUserSnapshot));
        _entityClient.update(newEntity, _systemAuthentication);
        log.debug(String.format("Successfully provisioned user %s", corpUserSnapshot.getUrn()));
      }
      log.debug(String.format("User %s already exists. Skipping provisioning", corpUserSnapshot.getUrn()));
      // Otherwise, the user exists. Skip provisioning.
    } catch (RemoteInvocationException e) {
      // Failing provisioning is something worth throwing about.
      throw new RuntimeException(String.format("Failed to provision user with urn %s.", corpUserSnapshot.getUrn()), e);
    }
  }

  private void tryProvisionGroups(List<CorpGroupSnapshot> corpGroups) {

    log.debug(String.format("Attempting to provision groups with urns %s",
        corpGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList())));

    // 1. Check if this user already exists.
    try {
      final Set<Urn> urnsToFetch = corpGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toSet());
      final Map<Urn, Entity> existingGroups = _entityClient.batchGet(urnsToFetch, _systemAuthentication);

      log.debug(String.format("Fetched GMS groups with urns %s", existingGroups.keySet()));

      final List<CorpGroupSnapshot> groupsToCreate = new ArrayList<>();
      for (CorpGroupSnapshot extractedGroup : corpGroups) {
        if (existingGroups.containsKey(extractedGroup.getUrn())) {

          final Entity groupEntity = existingGroups.get(extractedGroup.getUrn());
          final CorpGroupSnapshot corpGroupSnapshot = groupEntity.getValue().getCorpGroupSnapshot();

          // If more than the key aspect exists, then the group already "exists".
          if (corpGroupSnapshot.getAspects().size() <= 1) {
            log.debug(String.format("Extracted group that does not yet exist %s. Provisioning...",
                corpGroupSnapshot.getUrn()));
            groupsToCreate.add(extractedGroup);
          }
          log.debug(String.format("Group %s already exists. Skipping provisioning", corpGroupSnapshot.getUrn()));
        } else {
          // Should not occur until we stop returning default Key aspects for unrecognized entities.
          log.debug(
              String.format("Extracted group that does not yet exist %s. Provisioning...", extractedGroup.getUrn()));
          groupsToCreate.add(extractedGroup);
        }
      }

      List<Urn> groupsToCreateUrns =
          groupsToCreate.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList());

      log.debug(String.format("Provisioning groups with urns %s", groupsToCreateUrns));

      // Now batch create all entities identified to create.
      _entityClient.batchUpdate(groupsToCreate.stream()
          .map(groupSnapshot -> new Entity().setValue(Snapshot.create(groupSnapshot)))
          .collect(Collectors.toSet()), _systemAuthentication);

      log.debug(String.format("Successfully provisioned groups with urns %s", groupsToCreateUrns));
    } catch (RemoteInvocationException e) {
      // Failing provisioning is something worth throwing about.
      throw new RuntimeException(String.format("Failed to provision groups with urns %s.",
          corpGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList())), e);
    }
  }

  private void updateGroupMembership(Urn urn, GroupMembership groupMembership) {
    log.debug(String.format("Updating group membership for user %s", urn));
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(CORP_USER_ENTITY_NAME);
    proposal.setAspectName(GROUP_MEMBERSHIP_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(groupMembership));
    proposal.setChangeType(ChangeType.UPSERT);
    try {
      _entityClient.ingestProposal(proposal, _systemAuthentication);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(String.format("Failed to update group membership for user with urn %s", urn), e);
    }
  }

  private void verifyPreProvisionedUser(CorpuserUrn urn) {
    // Validate that the user exists in the system (there is more than just a key aspect for them, as of today).
    try {
      final Entity corpUser = _entityClient.get(urn, _systemAuthentication);

      log.debug(String.format("Fetched GMS user with urn %s", urn));

      // If we find more than the key aspect, then the entity "exists".
      if (corpUser.getValue().getCorpUserSnapshot().getAspects().size() <= 1) {
        log.debug(String.format("Found user that does not yet exist %s. Invalid login attempt. Throwing...", urn));
        throw new RuntimeException(String.format("User with urn %s has not yet been provisioned in DataHub. "
            + "Please contact your DataHub admin to provision an account.", urn));
      }
      // Otherwise, the user exists.
    } catch (RemoteInvocationException e) {
      // Failing validation is something worth throwing about.
      throw new RuntimeException(String.format("Failed to validate user with urn %s.", urn), e);
    }
  }

  private void setUserStatus(final Urn urn, final CorpUserStatus newStatus) throws Exception {
    // Update status aspect to be active.
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
    proposal.setAspectName(Constants.CORP_USER_STATUS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newStatus));
    proposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(proposal, _systemAuthentication);
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
