package auth.ldap;

import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for provisioning users and groups in DataHub from LDAP.
 *
 * <p>This singleton service provides methods to:
 *
 * <ul>
 *   <li><b>Provision users:</b> Create user profiles in DataHub from LDAP data
 *   <li><b>Provision groups:</b> Create group entities in DataHub from LDAP groups
 *   <li><b>Update group memberships:</b> Sync user-to-group relationships
 *   <li><b>Verify pre-provisioning:</b> Check if users have been pre-created
 *   <li><b>Set user status:</b> Mark users as active/inactive
 * </ul>
 *
 * <p><b>Singleton Pattern:</b>
 *
 * <p>This service uses the singleton pattern to ensure a single instance exists throughout the
 * application lifecycle. It must be initialized during application startup before any LDAP
 * authentication occurs.
 *
 * <p><b>Initialization:</b>
 *
 * <pre>
 * // In AuthModule.java during application startup
 * SystemEntityClient entityClient = ...;
 * OperationContext systemOperationContext = ...;
 * LdapProvisioningService.initialize(entityClient, systemOperationContext);
 * </pre>
 *
 * <p><b>Usage:</b>
 *
 * <pre>
 * // Get the singleton instance
 * LdapProvisioningService service = LdapProvisioningService.getInstance();
 *
 * // Provision a user
 * CorpUserSnapshot userSnapshot = ...;
 * service.tryProvisionUser(userSnapshot);
 *
 * // Provision groups
 * List&lt;CorpGroupSnapshot&gt; groups = ...;
 * service.tryProvisionGroups(groups);
 *
 * // Update user's group memberships
 * service.updateGroupMembership(userUrn, groups);
 *
 * // Set user status to active
 * CorpUserStatus activeStatus = new CorpUserStatus().setStatus(Constants.CORP_USER_STATUS_ACTIVE);
 * service.setUserStatus(userUrn, activeStatus);
 * </pre>
 *
 * <p><b>Provisioning Modes:</b>
 *
 * <p>This service supports two provisioning modes:
 *
 * <ol>
 *   <li><b>JIT (Just-In-Time) Provisioning:</b> Users and groups are automatically created during
 *       login. This is the default and recommended mode.
 *   <li><b>Pre-Provisioning:</b> Users must be created in DataHub before they can log in. The
 *       service verifies their existence during login.
 * </ol>
 *
 * <p><b>Entity Client:</b>
 *
 * <p>This service uses {@link SystemEntityClient} to interact with DataHub's metadata service
 * (GMS). All operations are performed with system-level privileges using the provided {@link
 * OperationContext}.
 *
 * <p><b>Error Handling:</b>
 *
 * <p>All methods throw {@link RuntimeException} or {@link Exception} if provisioning fails. This
 * ensures that authentication fails if users/groups cannot be properly provisioned, maintaining
 * data consistency.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. The singleton instance can be safely used by
 * multiple threads concurrently.
 *
 * @see SystemEntityClient
 * @see OperationContext
 * @see LdapProvisioningLoginModule
 */
@Slf4j
public class LdapProvisioningService {

  /** The singleton instance of this service. */
  private static LdapProvisioningService instance;

  /** Client for interacting with DataHub's metadata service (GMS). */
  private final SystemEntityClient entityClient;

  /**
   * Operation context with system-level privileges. Used for all metadata operations performed by
   * this service.
   */
  private final OperationContext operationContext;

  /**
   * Private constructor to enforce singleton pattern.
   *
   * @param entityClient The SystemEntityClient for metadata operations
   * @param operationContext The OperationContext with system privileges
   */
  private LdapProvisioningService(
      @Nonnull SystemEntityClient entityClient, @Nonnull OperationContext operationContext) {
    this.entityClient = entityClient;
    this.operationContext = operationContext;
  }

  /**
   * Creates a user-specific OperationContext for LDAP operations.
   *
   * <p>This method creates a fresh OperationContext with the authenticated user as the actor,
   * rather than using system authentication. This ensures that:
   *
   * <ul>
   *   <li>MCPs (MetadataChangeProposals) are properly generated with user attribution
   *   <li>Audit trails show the correct user who made the change
   *   <li>Kafka events contain proper user context
   * </ul>
   *
   * <p><b>Why this is needed:</b> The system OperationContext stored during initialization is meant
   * for internal operations, not user-initiated actions. Using system context for user operations
   * causes MCPs to fail generation or be rejected because they lack proper user attribution.
   *
   * @param username The username of the authenticated user
   * @return A fresh OperationContext with user-specific actor information
   */
  private OperationContext createUserOperationContext(@Nonnull String username) {
    // Create actor for the specific user
    Actor userActor = new Actor(ActorType.USER, username);
    Authentication userAuth = new Authentication(userActor, "LDAP", Collections.emptyMap());

    // Create a new OperationContext with user-specific actor but reusing other contexts
    // This ensures MCPs are generated with proper user attribution
    return this.operationContext.asSession(
        io.datahubproject.metadata.context.RequestContext.builder()
            .actorUrn(userActor.toUrnStr())
            .sourceIP("")
            .requestAPI(io.datahubproject.metadata.context.RequestContext.RequestAPI.RESTLI)
            .requestID("ldap-auth-" + username)
            .userAgent("LDAP"),
        this.operationContext.getAuthorizationContext().getAuthorizer(),
        userAuth);
  }

  /**
   * Initializes the singleton instance of LdapProvisioningService.
   *
   * <p>This method must be called during application startup, typically from {@code AuthModule},
   * before any LDAP authentication occurs. It should only be called once.
   *
   * <p><b>When to call:</b> During application initialization, after SystemEntityClient and
   * OperationContext are available.
   *
   * <p><b>Example:</b>
   *
   * <pre>
   * // In AuthModule.java
   * {@literal @}Provides
   * {@literal @}Singleton
   * protected SystemEntityClient provideEntityClient(...) {
   *     SystemEntityClient entityClient = new SystemRestliEntityClient(...);
   *
   *     // Initialize LDAP Provisioning Service
   *     try {
   *         LdapProvisioningService.initialize(entityClient, systemOperationContext);
   *         log.info("LDAP Provisioning Service initialized successfully");
   *     } catch (Exception e) {
   *         log.warn("Failed to initialize LDAP Provisioning Service", e);
   *     }
   *
   *     return entityClient;
   * }
   * </pre>
   *
   * <p><b>Thread Safety:</b> This method is synchronized to prevent race conditions during
   * initialization.
   *
   * @param entityClient The SystemEntityClient for metadata operations (must not be null)
   * @param operationContext The OperationContext with system privileges (must not be null)
   */
  public static synchronized void initialize(
      @Nonnull SystemEntityClient entityClient, @Nonnull OperationContext operationContext) {
    if (instance == null) {
      instance = new LdapProvisioningService(entityClient, operationContext);
      log.info("LdapProvisioningService initialized successfully");
    } else {
      log.warn("LdapProvisioningService already initialized");
    }
  }

  /**
   * Gets the singleton instance of LdapProvisioningService.
   *
   * <p>This method returns the singleton instance that was created during initialization. It must
   * be called after {@link #initialize(SystemEntityClient, OperationContext)} has been called.
   *
   * <p><b>Example:</b>
   *
   * <pre>
   * // In LdapProvisioningLoginModule
   * LdapProvisioningService service = LdapProvisioningService.getInstance();
   * service.tryProvisionUser(userSnapshot);
   * </pre>
   *
   * @return The singleton instance of LdapProvisioningService
   * @throws IllegalStateException if the service has not been initialized yet
   */
  public static LdapProvisioningService getInstance() {
    if (instance == null) {
      throw new IllegalStateException(
          "LdapProvisioningService has not been initialized. Call initialize() first.");
    }
    return instance;
  }

  /**
   * Provisions a user in DataHub if they don't already exist.
   *
   * <p>This method checks if the user already exists in DataHub. If the user doesn't exist (i.e.,
   * only has a key aspect), it creates the user with the provided information. If the user already
   * exists with full profile data, it skips provisioning.
   *
   * <p><b>How it works:</b>
   *
   * <ol>
   *   <li>Fetches the user entity from DataHub using the URN
   *   <li>Checks if the user has more than just the key aspect (key aspect is auto-generated)
   *   <li>If only key aspect exists, provisions the user with full profile data
   *   <li>If user already has profile data, skips provisioning
   * </ol>
   *
   * <p><b>What gets provisioned:</b>
   *
   * <ul>
   *   <li>User URN (username)
   *   <li>Full name
   *   <li>Display name
   *   <li>Email address
   *   <li>Active status
   * </ul>
   *
   * <p><b>Example:</b>
   *
   * <pre>
   * CorpUserSnapshot userSnapshot = new CorpUserSnapshot();
   * userSnapshot.setUrn(new CorpuserUrn("jdoe"));
   * // ... set user info aspects ...
   *
   * service.tryProvisionUser(userSnapshot);
   * // User is now created in DataHub (if didn't exist)
   * </pre>
   *
   * <p><b>Idempotent:</b> This method is idempotent - calling it multiple times with the same user
   * will not create duplicates or cause errors.
   *
   * @param corpUserSnapshot The user snapshot containing user information from LDAP
   * @throws RuntimeException if provisioning fails due to communication errors with GMS
   */
  public void tryProvisionUser(@Nonnull CorpUserSnapshot corpUserSnapshot) {
    log.debug("Attempting to provision user with urn {}", corpUserSnapshot.getUrn());

    try {
      // Check if this user already exists
      final Entity corpUser = entityClient.get(operationContext, corpUserSnapshot.getUrn());
      final CorpUserSnapshot existingCorpUserSnapshot = corpUser.getValue().getCorpUserSnapshot();

      log.debug("Fetched GMS user with urn {}", corpUserSnapshot.getUrn());

      // If we find more than the key aspect, then the entity "exists"
      if (existingCorpUserSnapshot.getAspects().size() <= 1) {
        log.debug(
            "Extracted user that does not yet exist {}. Provisioning...",
            corpUserSnapshot.getUrn());
        // The user does not exist. Provision them.
        final Entity newEntity = new Entity();
        newEntity.setValue(Snapshot.create(corpUserSnapshot));
        entityClient.update(operationContext, newEntity);
        log.info("Successfully provisioned user {}", corpUserSnapshot.getUrn());
      } else {
        log.debug("User {} already exists. Skipping provisioning", corpUserSnapshot.getUrn());
      }
    } catch (RemoteInvocationException e) {
      // Failing provisioning is something worth throwing about
      throw new RuntimeException(
          String.format("Failed to provision user with urn %s.", corpUserSnapshot.getUrn()), e);
    }
  }

  /**
   * Provisions multiple groups in DataHub if they don't already exist.
   *
   * <p>This method performs batch provisioning of groups for efficiency. It checks which groups
   * already exist and only provisions the ones that don't.
   *
   * <p><b>How it works:</b>
   *
   * <ol>
   *   <li>Extracts URNs from all group snapshots
   *   <li>Performs batch fetch to check which groups exist in DataHub
   *   <li>Filters out groups that already have full profile data
   *   <li>Batch creates all groups that don't exist
   * </ol>
   *
   * <p><b>What gets provisioned:</b>
   *
   * <ul>
   *   <li>Group URN (group name)
   *   <li>Display name
   *   <li>Description (if available)
   *   <li>Empty members list (updated separately via {@link #updateGroupMembership})
   * </ul>
   *
   * <p><b>Example:</b>
   *
   * <pre>
   * List&lt;CorpGroupSnapshot&gt; groups = new ArrayList&lt;&gt;();
   * // ... add group snapshots from LDAP ...
   *
   * service.tryProvisionGroups(groups);
   * // All non-existing groups are now created in DataHub
   * </pre>
   *
   * <p><b>Performance:</b> This method uses batch operations for efficiency. It's optimized to
   * handle multiple groups in a single call.
   *
   * <p><b>Idempotent:</b> This method is idempotent - calling it multiple times with the same
   * groups will not create duplicates or cause errors.
   *
   * @param corpGroups List of group snapshots containing group information from LDAP
   * @throws RuntimeException if provisioning fails due to communication errors with GMS
   */
  public void tryProvisionGroups(@Nonnull List<CorpGroupSnapshot> corpGroups) {
    log.debug(
        "Attempting to provision groups with urns {}",
        corpGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList()));

    try {
      final Set<Urn> urnsToFetch =
          corpGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toSet());
      final Map<Urn, Entity> existingGroups = entityClient.batchGet(operationContext, urnsToFetch);

      log.debug("Fetched GMS groups with urns {}", existingGroups.keySet());

      final List<CorpGroupSnapshot> groupsToCreate =
          corpGroups.stream()
              .filter(
                  extractedGroup -> {
                    if (existingGroups.containsKey(extractedGroup.getUrn())) {
                      final Entity groupEntity = existingGroups.get(extractedGroup.getUrn());
                      final CorpGroupSnapshot corpGroupSnapshot =
                          groupEntity.getValue().getCorpGroupSnapshot();

                      // If more than the key aspect exists, then the group already "exists"
                      if (corpGroupSnapshot.getAspects().size() <= 1) {
                        log.debug(
                            "Extracted group that does not yet exist {}. Will provision...",
                            corpGroupSnapshot.getUrn());
                        return true;
                      }
                      log.debug(
                          "Group {} already exists. Skipping provisioning",
                          corpGroupSnapshot.getUrn());
                      return false;
                    } else {
                      // Should not occur until we stop returning default Key aspects for
                      // unrecognized entities
                      log.debug(
                          "Extracted group that does not yet exist {}. Will provision...",
                          extractedGroup.getUrn());
                      return true;
                    }
                  })
              .collect(Collectors.toList());

      if (!groupsToCreate.isEmpty()) {
        List<Urn> groupsToCreateUrns =
            groupsToCreate.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList());

        log.debug("Provisioning groups with urns {}", groupsToCreateUrns);

        // Batch create all entities identified to create
        entityClient.batchUpdate(
            operationContext,
            groupsToCreate.stream()
                .map(groupSnapshot -> new Entity().setValue(Snapshot.create(groupSnapshot)))
                .collect(Collectors.toSet()));

        log.info("Successfully provisioned groups with urns {}", groupsToCreateUrns);
      }
    } catch (RemoteInvocationException e) {
      // Failing provisioning is something worth throwing about
      throw new RuntimeException(
          String.format(
              "Failed to provision groups with urns %s.",
              corpGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  /**
   * Updates group membership for a user in DataHub.
   *
   * <p><b>Important:</b> This method replaces the user's entire group membership list. It does not
   * append to existing memberships - it performs a complete replacement.
   *
   * <p><b>How it works:</b>
   *
   * <ol>
   *   <li>Creates a fresh user-specific OperationContext
   *   <li>Creates a MetadataChangeProposal with UPSERT change type
   *   <li>Ingests the proposal using the user context to update the user's group membership
   * </ol>
   *
   * <p><b>Example:</b>
   *
   * <pre>
   * String username = "jdoe";
   * CorpuserUrn userUrn = new CorpuserUrn(username);
   * GroupMembership groupMembership = ...; // GroupMembership aspect
   *
   * service.updateGroupMembership(username, userUrn, groupMembership);
   * // User's group memberships are now synced with LDAP
   * </pre>
   *
   * <p><b>Use Case:</b> This method is typically called after provisioning groups to establish the
   * user-to-group relationships extracted from LDAP.
   *
   * <p><b>Synchronization:</b> This ensures the user's group memberships in DataHub match their
   * LDAP group memberships. Any groups the user was previously in (but not in the new list) will be
   * removed.
   *
   * <p><b>MCP Generation:</b> By using a user-specific OperationContext, this method ensures that
   * MCPs are properly generated with correct user attribution, enabling proper audit trails and
   * Kafka event generation.
   *
   * @param username The username of the authenticated user (used to create user context)
   * @param userUrn The URN of the user whose group membership to update
   * @param groupMembership The GroupMembership aspect containing the groups
   * @throws RuntimeException if the update fails due to communication errors with GMS
   */
  public void updateGroupMembership(
      @Nonnull String username, @Nonnull Urn userUrn, @Nonnull GroupMembership groupMembership) {
    log.debug("Updating group membership for user {}", userUrn);

    // Create fresh user-specific OperationContext
    OperationContext userOpContext = createUserOperationContext(username);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(userUrn);
    proposal.setEntityType(CORP_USER_ENTITY_NAME);
    proposal.setAspectName(GROUP_MEMBERSHIP_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(groupMembership));
    proposal.setChangeType(ChangeType.UPSERT);

    try {
      // Use user-specific context instead of system context
      entityClient.ingestProposal(userOpContext, proposal);
      log.info(
          "Successfully updated group membership for user {} with {} groups",
          userUrn,
          groupMembership.getGroups().size());
    } catch (RemoteInvocationException e) {
      log.error("Failed to update group membership for user {}: {}", userUrn, e.getMessage(), e);
      throw new RuntimeException(
          String.format("Failed to update group membership for user with urn %s", userUrn), e);
    }
  }

  /**
   * Verifies that a user has been pre-provisioned in DataHub.
   *
   * <p>This method is used when pre-provisioning mode is enabled. It checks if the user already
   * exists in DataHub with full profile data. If the user doesn't exist or only has a key aspect,
   * it throws an exception to prevent login.
   *
   * <p><b>How it works:</b>
   *
   * <ol>
   *   <li>Fetches the user entity from DataHub
   *   <li>Checks if the user has more than just the key aspect
   *   <li>If user doesn't exist or only has key aspect, throws RuntimeException
   *   <li>If user exists with full profile, verification succeeds
   * </ol>
   *
   * <p><b>Use Case:</b> This is used in environments where users must be manually created in
   * DataHub before they can log in via LDAP. This provides more control over who can access
   * DataHub.
   *
   * <p><b>Example:</b>
   *
   * <pre>
   * // In LdapProvisioningLoginModule when preProvisioningRequired=true
   * CorpuserUrn userUrn = new CorpuserUrn("jdoe");
   *
   * try {
   *     service.verifyPreProvisionedUser(userUrn);
   *     // User exists, allow login to proceed
   * } catch (RuntimeException e) {
   *     // User not pre-provisioned, deny login
   *     throw new LoginException("User not authorized");
   * }
   * </pre>
   *
   * <p><b>Configuration:</b> This method is only called when {@code
   * auth.ldap.preProvisioningRequired=true} in the configuration.
   *
   * @param urn The URN of the user to verify
   * @throws RuntimeException if the user has not been pre-provisioned or if verification fails
   */
  public void verifyPreProvisionedUser(@Nonnull CorpuserUrn urn) {
    log.debug("Verifying pre-provisioned user {}", urn);

    try {
      final Entity corpUser = entityClient.get(operationContext, urn);

      log.debug("Fetched GMS user with urn {}", urn);

      // If we find more than the key aspect, then the entity "exists"
      if (corpUser.getValue().getCorpUserSnapshot().getAspects().size() <= 1) {
        log.warn("User {} has not been pre-provisioned", urn);
        throw new RuntimeException(
            String.format(
                "User with urn %s has not yet been provisioned in DataHub. "
                    + "Please contact your DataHub admin to provision an account.",
                urn));
      }
      log.debug("User {} is pre-provisioned", urn);
    } catch (RemoteInvocationException e) {
      // Failing validation is something worth throwing about
      throw new RuntimeException(String.format("Failed to validate user with urn %s.", urn), e);
    }
  }

  /**
   * Sets the status of a user in DataHub.
   *
   * <p>This method updates the user's status aspect, typically to mark them as active after
   * successful login. The status can also be used to deactivate users.
   *
   * <p><b>How it works:</b>
   *
   * <ol>
   *   <li>Creates a fresh user-specific OperationContext
   *   <li>Creates a CorpUserStatus aspect with the new status
   *   <li>Creates a MetadataChangeProposal with UPSERT change type
   *   <li>Ingests the proposal using the user context to update the user's status
   * </ol>
   *
   * <p><b>Common Status Values:</b>
   *
   * <ul>
   *   <li>{@code Constants.CORP_USER_STATUS_ACTIVE} - User is active and can access DataHub
   *   <li>{@code Constants.CORP_USER_STATUS_INACTIVE} - User is deactivated
   * </ul>
   *
   * <p><b>Example:</b>
   *
   * <pre>
   * // Mark user as active after successful login
   * String username = "jdoe";
   * CorpuserUrn userUrn = new CorpuserUrn(username);
   * CorpUserStatus activeStatus = new CorpUserStatus()
   *     .setStatus(Constants.CORP_USER_STATUS_ACTIVE)
   *     .setLastModified(new AuditStamp()
   *         .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
   *         .setTime(System.currentTimeMillis()));
   *
   * service.setUserStatus(username, userUrn, activeStatus);
   * </pre>
   *
   * <p><b>Use Case:</b> This is typically called at the end of the provisioning process to mark the
   * user as active. It can also be used to deactivate users who should no longer have access.
   *
   * <p><b>Audit Trail:</b> The status includes an audit stamp with the actor (who made the change)
   * and timestamp, providing an audit trail of status changes.
   *
   * <p><b>MCP Generation:</b> By using a user-specific OperationContext, this method ensures that
   * MCPs are properly generated with correct user attribution.
   *
   * @param username The username of the authenticated user (used to create user context)
   * @param urn The URN of the user whose status to update
   * @param newStatus The new status to set (including audit information)
   * @throws Exception if the update fails due to communication errors with GMS
   */
  public void setUserStatus(
      @Nonnull String username, @Nonnull final Urn urn, @Nonnull final CorpUserStatus newStatus)
      throws Exception {
    log.debug("Setting user status for {} to {}", urn, newStatus.getStatus());

    // Create fresh user-specific OperationContext
    OperationContext userOpContext = createUserOperationContext(username);

    // Update status aspect to be active
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
    proposal.setAspectName(Constants.CORP_USER_STATUS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newStatus));
    proposal.setChangeType(ChangeType.UPSERT);

    try {
      // Use user-specific context instead of system context
      entityClient.ingestProposal(userOpContext, proposal);
      log.info("Successfully set user status for {}", urn);
    } catch (RemoteInvocationException e) {
      throw new Exception(String.format("Failed to set user status for urn %s", urn), e);
    }
  }
}
