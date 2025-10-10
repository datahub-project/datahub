package auth.ldap;

import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.aspect.CorpGroupAspectArray;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import security.AuthenticationManager;

/**
 * Logic for provisioning DataHub users, groups, and memberships after LDAP authentication. Similar
 * to OidcCallbackLogic but for LDAP/JAAS authentication.
 */
public class LdapProvisioningLogic {
  private static final Logger log = LoggerFactory.getLogger(LdapProvisioningLogic.class);

  private final SystemEntityClient systemEntityClient;
  private final OperationContext systemOperationContext;

  public LdapProvisioningLogic(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final OperationContext systemOperationContext) {
    this.systemEntityClient = systemEntityClient;
    this.systemOperationContext = systemOperationContext;
  }

  /**
   * Provisions a user, groups, and memberships after successful LDAP authentication.
   *
   * @param username The authenticated username
   * @param groups The groups the user belongs to
   * @param userDN The user's distinguished name (optional, may be null)
   * @param ldapOptions LDAP configuration options
   * @return The provisioned user URN
   */
  public String provisionUser(
      @Nonnull String username,
      @Nonnull String password,
      @Nonnull Set<String> groups,
      String userDN,
      Map<String, String> ldapOptions) {

    try {
      // Create user URN
      final String userUrnString = new CorpuserUrn(username).toString();
      log.debug("Provisioning user with URN: {}", userUrnString);

      // Extract user attributes from LDAP if DN is available
      Map<String, String> userAttributes = new HashMap<>();
      if (userDN != null && ldapOptions != null) {
        try {
          userAttributes =
              AuthenticationManager.getUserAttributesFromLdap(
                  userDN, ldapOptions, username, password);
          log.debug("Fetched LDAP attributes for user {}: {}", username, userAttributes);
        } catch (Exception e) {
          log.warn("Failed to fetch LDAP attributes for user {}: {}", username, e.getMessage());
        }
      }

      // Create user snapshot
      CorpUserSnapshot userSnapshot = buildCorpUserSnapshot(username, userAttributes);

      // Provision the user
      provisionUser(userSnapshot);

      // Provision groups
      List<CorpGroupSnapshot> groupSnapshots =
          groups.stream().map(this::buildCorpGroupSnapshot).collect(Collectors.toList());
      provisionGroups(groupSnapshots);

      // Update group memberships
      updateGroupMembership(userUrnString, groups);

      return userUrnString;

    } catch (Exception e) {
      throw new RuntimeException("Failed to provision LDAP user", e);
    }
  }

  private CorpUserSnapshot buildCorpUserSnapshot(String username, Map<String, String> attributes) {
    final CorpUserSnapshot userSnapshot = new CorpUserSnapshot();
    userSnapshot.setUrn(new CorpuserUrn(username));

    final CorpUserAspectArray aspects = new CorpUserAspectArray();

    // Build CorpUserInfo aspect
    final CorpUserInfo userInfo = new CorpUserInfo();
    userInfo.setActive(true);

    // Extract name information
    String displayName = attributes.getOrDefault("displayName", username);
    String givenName = attributes.getOrDefault("givenName", "");
    String surname = attributes.getOrDefault("sn", "");
    String fullName = displayName;
    if (fullName.equals(username) && !givenName.isEmpty() && !surname.isEmpty()) {
      fullName = givenName + " " + surname;
    }

    userInfo.setFullName(fullName, SetMode.IGNORE_NULL);
    userInfo.setDisplayName(displayName, SetMode.IGNORE_NULL);

    // Set email
    String email =
        attributes.getOrDefault("mail", attributes.getOrDefault("userPrincipalName", ""));
    if (!email.isEmpty()) {
      userInfo.setEmail(email, SetMode.IGNORE_NULL);
    }

    // Add the aspect
    aspects.add(CorpUserAspect.create(userInfo));

    // Build the key
    final CorpUserKey userKey = new CorpUserKey();
    userKey.setUsername(username);
    aspects.add(CorpUserAspect.create(userKey));

    userSnapshot.setAspects(aspects);
    return userSnapshot;
  }

  private CorpGroupSnapshot buildCorpGroupSnapshot(String groupName) {
    final CorpGroupSnapshot groupSnapshot = new CorpGroupSnapshot();
    groupSnapshot.setUrn(new CorpGroupUrn(groupName));

    final CorpGroupAspectArray aspects = new CorpGroupAspectArray();

    // Build CorpGroupInfo aspect
    final CorpGroupInfo groupInfo = new CorpGroupInfo();
    groupInfo.setDisplayName(groupName);
    groupInfo.setDescription("LDAP Group: " + groupName);
    groupInfo.setMembers(new CorpuserUrnArray());
    groupInfo.setGroups(new CorpGroupUrnArray());
    groupInfo.setAdmins(new CorpuserUrnArray());

    aspects.add(CorpGroupAspect.create(groupInfo));

    // Build the key
    final CorpGroupKey groupKey = new CorpGroupKey();
    groupKey.setName(groupName);
    aspects.add(CorpGroupAspect.create(groupKey));

    groupSnapshot.setAspects(aspects);
    return groupSnapshot;
  }

  private void provisionUser(CorpUserSnapshot userSnapshot) {
    try {
      Urn userUrn = userSnapshot.getUrn();

      // Check if user already exists
      if (systemEntityClient.exists(systemOperationContext, userUrn)) {
        log.debug("User {} already exists. Skipping provisioning", userUrn);
        return;
      }

      // Create the user
      final Entity newEntity = new Entity();
      newEntity.setValue(Snapshot.create(userSnapshot));
      systemEntityClient.update(systemOperationContext, newEntity);
      log.info("Successfully provisioned user {}", userUrn);

    } catch (RemoteInvocationException e) {
      throw new RuntimeException("Failed to provision user", e);
    }
  }

  private void provisionGroups(List<CorpGroupSnapshot> groupSnapshots) {
    try {
      final Map<String, Entity> entities = new HashMap<>();

      for (CorpGroupSnapshot groupSnapshot : groupSnapshots) {
        Urn groupUrn = groupSnapshot.getUrn();

        // Check if group already exists
        if (!systemEntityClient.exists(systemOperationContext, groupUrn)) {
          final Entity newEntity = new Entity();
          newEntity.setValue(Snapshot.create(groupSnapshot));
          entities.put(groupUrn.toString(), newEntity);
        } else {
          log.debug("Group {} already exists. Skipping provisioning", groupUrn);
        }
      }

      if (!entities.isEmpty()) {
        systemEntityClient.batchUpdate(systemOperationContext, new HashSet<>(entities.values()));
        log.info("Successfully provisioned {} groups", entities.size());
      }

    } catch (RemoteInvocationException e) {
      throw new RuntimeException("Failed to provision groups", e);
    }
  }

  private void updateGroupMembership(String userUrnString, Set<String> groupNames) {
    try {
      final GroupMembership groupMembership = new GroupMembership();
      final UrnArray groupUrns = new UrnArray();

      for (String groupName : groupNames) {
        groupUrns.add(new CorpGroupUrn(groupName));
      }

      groupMembership.setGroups(groupUrns);

      // Create metadata change proposal
      final MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(Urn.createFromString(userUrnString));
      proposal.setEntityType("corpuser");
      proposal.setAspectName("groupMembership");
      proposal.setAspect(GenericRecordUtils.serializeAspect(groupMembership));
      proposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);

      systemEntityClient.ingestProposal(systemOperationContext, proposal, false);
      log.info("Successfully updated group membership for user {}", userUrnString);

    } catch (Exception e) {
      log.error("Failed to update group membership for user {}", userUrnString, e);
    }
  }
}
