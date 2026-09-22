package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ROLE_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationSubType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.test.metadata.aspect.batch.TestPatchMCP;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PrivilegeGrantAuthorizationValidatorTest {

  private static final String MANAGE_POLICIES = "MANAGE_POLICIES";
  private static final String MANAGE_USERS_AND_GROUPS = "MANAGE_USERS_AND_GROUPS";
  private static final String EDIT_GROUP_MEMBERS = "EDIT_GROUP_MEMBERS";
  private static final String EDIT_ENTITY_OWNERS = "EDIT_ENTITY_OWNERS";

  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:actor");
  private static final Urn OTHER_USER = UrnUtils.getUrn("urn:li:corpuser:other");
  private static final Urn GROUP = UrnUtils.getUrn("urn:li:corpGroup:privileged");
  private static final Urn ADMIN_ROLE = UrnUtils.getUrn("urn:li:dataHubRole:Admin");
  private static final Urn OTHER_GROUP = UrnUtils.getUrn("urn:li:corpGroup:other");
  private static final String ANY_RESOURCE = "*";

  private final Map<String, Set<String>> grantedPrivileges = new HashMap<>();

  private AspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;
  private AuthorizationSession session;
  private OperationFingerprint operationContext;
  private PrivilegeGrantAuthorizationValidator validator;

  @BeforeMethod
  public void setUp() {
    aspectRetriever = mock(AspectRetriever.class);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(aspectRetriever);
    session = mock(AuthorizationSession.class);
    operationContext = mock(OperationFingerprint.class);

    validator =
        new PrivilegeGrantAuthorizationValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className(PrivilegeGrantAuthorizationValidator.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT", "PATCH"))
                    .supportedEntityAspectNames(List.of())
                    .build());

    grantedPrivileges.clear();
    stubAuthorization();
  }

  /** Stubs the session from {@link #grantedPrivileges}, honouring the resource the check names. */
  private void stubAuthorization() {
    when(session.authorize(anyString(), any()))
        .thenAnswer(
            args -> {
              final String privilege = args.getArgument(0);
              final EntitySpec resource = args.getArgument(1);
              final Set<String> allowedOn = grantedPrivileges.getOrDefault(privilege, Set.of());
              final boolean allowed =
                  allowedOn.contains(ANY_RESOURCE)
                      || (resource != null && allowedOn.contains(resource.getEntity()));
              return new AuthorizationResult(
                  null,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  null);
            });
  }

  /** Allow these privileges on any resource, as a platform or unfiltered policy would. */
  private void grantPrivileges(String... privileges) {
    for (String privilege : privileges) {
      grantedPrivileges.computeIfAbsent(privilege, key -> new HashSet<>()).add(ANY_RESOURCE);
    }
  }

  /** Allow a privilege on one resource only, as a resource-scoped policy would. */
  private void grantPrivilegeOn(String privilege, Urn resource) {
    grantedPrivileges.computeIfAbsent(privilege, key -> new HashSet<>()).add(resource.toString());
  }

  private void existingAspect(Urn urn, String aspectName, RecordTemplate aspect) {
    when(aspectRetriever.getLatestAspectObjects(any(), anySet(), anySet()))
        .thenAnswer(
            args -> {
              final Set<Urn> urns = args.getArgument(1);
              final Set<String> aspectNames = args.getArgument(2);
              return urns.contains(urn) && aspectNames.contains(aspectName)
                  ? Map.of(urn, Map.of(aspectName, new Aspect(aspect.data())))
                  : Map.of();
            });
  }

  private BatchItem upsert(Urn urn, String aspectName, RecordTemplate aspect, Urn actor) {
    return item(urn, aspectName, aspect, actor, ChangeType.UPSERT);
  }

  private BatchItem item(
      Urn urn, String aspectName, RecordTemplate aspect, Urn actor, ChangeType changeType) {
    final com.linkedin.metadata.models.EntitySpec entitySpec =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    when(entitySpec.getName()).thenReturn(urn.getEntityType());
    final AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn(aspectName);

    return TestMCP.builder()
        .urn(urn)
        .changeType(changeType)
        .entitySpec(entitySpec)
        .aspectSpec(aspectSpec)
        .recordTemplate(aspect)
        .auditStamp(new AuditStamp().setActor(actor).setTime(0L))
        .build();
  }

  private List<AspectValidationException> validate(BatchItem... items) {
    final List<BatchItem> batch = List.of(items);
    return validator.validateItems(operationContext, batch, batch, retrieverContext, session);
  }

  private static RoleMembership roles(Urn... roleUrns) {
    return new RoleMembership().setRoles(new UrnArray(List.of(roleUrns)));
  }

  private static GroupMembership groups(Urn... groupUrns) {
    return new GroupMembership().setGroups(new UrnArray(List.of(groupUrns)));
  }

  private static Ownership owners(Urn... ownerUrns) {
    final OwnerArray ownerArray = new OwnerArray();
    for (Urn ownerUrn : ownerUrns) {
      ownerArray.add(new Owner().setOwner(ownerUrn).setType(OwnershipType.TECHNICAL_OWNER));
    }
    return new Ownership().setOwners(ownerArray);
  }

  private static void assertAuthFailure(List<AspectValidationException> failures) {
    assertEquals(failures.size(), 1, "Expected exactly one failure, got: " + failures);
    assertEquals(
        failures.get(0).getSubType(),
        ValidationSubType.AUTHORIZATION,
        "Denials must surface as authorization failures, not validation errors");
  }

  // --- roleMembership: Manage Policies floor, no self check ---

  @Test
  public void testRoleGrantDeniedWithoutManagePolicies() {
    grantPrivileges("EDIT_ENTITY", "UPDATE_USERS");
    assertAuthFailure(
        validate(upsert(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR)));
  }

  @Test
  public void testRoleGrantAllowedWithManagePolicies() {
    grantPrivileges(MANAGE_POLICIES);
    assertTrue(
        validate(upsert(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR))
            .isEmpty());
  }

  /**
   * An admin re-assigning their own role must not be blocked - that is friction without security.
   */
  @Test
  public void testSelfRoleGrantAllowedWithManagePolicies() {
    grantPrivileges(MANAGE_POLICIES);
    assertTrue(
        validate(upsert(ACTOR, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR)).isEmpty());
  }

  @Test
  public void testSelfRoleGrantDeniedWithoutManagePolicies() {
    grantPrivileges("EDIT_ENTITY");
    assertAuthFailure(
        validate(upsert(ACTOR, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR)));
  }

  @Test
  public void testRoleGrantOnGroupDeniedWithOwnershipDerivedPrivileges() {
    grantPrivileges("EDIT_ENTITY", EDIT_GROUP_MEMBERS);
    assertAuthFailure(
        validate(upsert(GROUP, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR)));
  }

  // --- group membership: floor rises for self grants ---

  @Test
  public void testGroupGrantToOtherAllowedWithEditGroupMembers() {
    grantPrivileges(EDIT_GROUP_MEMBERS);
    assertTrue(
        validate(upsert(OTHER_USER, GROUP_MEMBERSHIP_ASPECT_NAME, groups(GROUP), ACTOR)).isEmpty());
  }

  @Test
  public void testSelfGroupGrantDeniedWithEditGroupMembersOnly() {
    grantPrivileges(EDIT_GROUP_MEMBERS);
    assertAuthFailure(validate(upsert(ACTOR, GROUP_MEMBERSHIP_ASPECT_NAME, groups(GROUP), ACTOR)));
  }

  @Test
  public void testSelfGroupGrantAllowedWithManageUsersAndGroups() {
    grantPrivileges(MANAGE_USERS_AND_GROUPS);
    assertTrue(
        validate(upsert(ACTOR, GROUP_MEMBERSHIP_ASPECT_NAME, groups(GROUP), ACTOR)).isEmpty());
  }

  /**
   * EDIT_GROUP_MEMBERS is held on the group - the Asset Owners policy scopes it to owned entities -
   * while the membership aspect is written on the member's corpuser entity. Authorizing against the
   * aspect's own URN would deny a legitimate group owner.
   */
  @Test
  public void testGroupGrantAuthorizedAgainstGroupNotMember() {
    grantPrivilegeOn(EDIT_GROUP_MEMBERS, GROUP);
    assertTrue(
        validate(upsert(OTHER_USER, GROUP_MEMBERSHIP_ASPECT_NAME, groups(GROUP), ACTOR)).isEmpty());
  }

  @Test
  public void testGroupGrantDeniedWhenOnlyMemberUrnAuthorized() {
    grantPrivilegeOn(EDIT_GROUP_MEMBERS, OTHER_USER);
    assertAuthFailure(
        validate(upsert(OTHER_USER, GROUP_MEMBERSHIP_ASPECT_NAME, groups(GROUP), ACTOR)));
  }

  /** One editable group must not carry an unauthorized group along in the same write. */
  @Test
  public void testMultiGroupGrantRequiresEveryGroup() {
    grantPrivilegeOn(EDIT_GROUP_MEMBERS, GROUP);
    assertAuthFailure(
        validate(
            upsert(OTHER_USER, GROUP_MEMBERSHIP_ASPECT_NAME, groups(GROUP, OTHER_GROUP), ACTOR)));
  }

  @Test
  public void testMultiGroupGrantAllowedWhenEveryGroupAuthorized() {
    grantPrivilegeOn(EDIT_GROUP_MEMBERS, GROUP);
    grantPrivilegeOn(EDIT_GROUP_MEMBERS, OTHER_GROUP);
    assertTrue(
        validate(
                upsert(OTHER_USER, GROUP_MEMBERSHIP_ASPECT_NAME, groups(GROUP, OTHER_GROUP), ACTOR))
            .isEmpty());
  }

  // --- corpGroup ownership: self grant is the entry point to the Asset Owners chain ---

  @Test
  public void testSelfOwnershipGrantOnGroupDeniedWithEditOwnersOnly() {
    grantPrivileges(EDIT_ENTITY_OWNERS);
    assertAuthFailure(validate(upsert(GROUP, OWNERSHIP_ASPECT_NAME, owners(ACTOR), ACTOR)));
  }

  @Test
  public void testOwnershipGrantToOtherOnGroupAllowedWithEditOwners() {
    grantPrivileges(EDIT_ENTITY_OWNERS);
    assertTrue(validate(upsert(GROUP, OWNERSHIP_ASPECT_NAME, owners(OTHER_USER), ACTOR)).isEmpty());
  }

  // --- rules key off the aspect, not the entity type ---

  /**
   * AspectPluginConfig matches entity names and aspect names in two independent passes, so a rule
   * reaches any registered entity type carrying its aspect. That breadth is deliberate for a
   * security control: were corpuser ever to gain an ownership aspect, the self-grant floor should
   * apply there too rather than silently lapse.
   */
  @Test
  public void testOwnershipRuleAppliesToAnyEntityCarryingTheAspect() {
    grantPrivileges(EDIT_ENTITY_OWNERS);
    assertAuthFailure(validate(upsert(OTHER_USER, OWNERSHIP_ASPECT_NAME, owners(ACTOR), ACTOR)));
  }

  // --- only additions count as grants ---

  @Test
  public void testRoleRemovalAllowedWithoutPrivileges() {
    existingAspect(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE));
    assertTrue(validate(upsert(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, roles(), ACTOR)).isEmpty());
  }

  @Test
  public void testUnchangedRoleReingestAllowedWithoutPrivileges() {
    existingAspect(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE));
    assertTrue(
        validate(upsert(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR))
            .isEmpty());
  }

  /** An unreadable proposal must demand the strictest floor rather than passing unchecked. */
  @Test
  public void testUnreadableProposalFailsClosed() {
    assertAuthFailure(validate(upsert(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, null, ACTOR)));
  }

  @Test
  public void testUnreadableProposalAllowedForPrivilegedActor() {
    grantPrivileges(MANAGE_POLICIES);
    assertTrue(validate(upsert(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, null, ACTOR)).isEmpty());
  }

  /** Removing an aspect can only reduce privileges. */
  @Test
  public void testDeleteAllowedWithoutPrivileges() {
    assertTrue(
        validate(item(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, null, ACTOR, ChangeType.DELETE))
            .isEmpty());
  }

  // --- current state is read in one call per entity type, not one per item ---

  /**
   * A batched membership sync must not pay a serial read per user. Entity types are fetched
   * separately because the retriever scopes the query to the first URN's entity type.
   */
  @Test
  public void testCurrentStateReadIsBatchedPerEntityType() {
    grantPrivileges(MANAGE_POLICIES);
    when(aspectRetriever.getLatestAspectObjects(any(), anySet(), anySet())).thenReturn(Map.of());

    assertTrue(
        validate(
                upsert(ACTOR, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR),
                upsert(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR),
                upsert(GROUP, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR))
            .isEmpty());

    // two corpusers collapse into one read; the corpGroup needs its own
    verify(aspectRetriever, times(2)).getLatestAspectObjects(any(), anySet(), anySet());
    verify(aspectRetriever, never()).getLatestAspectObject(any(), any(Urn.class), anyString());
  }

  /** Items for aspects with no rule must not trigger a read at all. */
  @Test
  public void testUnguardedAspectsTriggerNoRead() {
    assertTrue(validate(upsert(ACTOR, "corpUserInfo", roles(ADMIN_ROLE), ACTOR)).isEmpty());
    verify(aspectRetriever, never()).getLatestAspectObjects(any(), anySet(), anySet());
  }

  // --- trusted internal writes ---

  @Test
  public void testSystemUpdateSourceSkipped() {
    final BatchItem item =
        upsert(OTHER_USER, ROLE_MEMBERSHIP_ASPECT_NAME, roles(ADMIN_ROLE), ACTOR);
    ((TestMCP) item)
        .setSystemMetadata(
            new SystemMetadata()
                .setProperties(new StringMap(java.util.Map.of(APP_SOURCE, SYSTEM_UPDATE_SOURCE))));
    assertTrue(validate(item).isEmpty());
  }

  /** The invite-accept flow assigns roles as the system actor. */
  @Test
  public void testSystemActorSkipped() {
    assertTrue(
        validate(
                upsert(
                    ACTOR,
                    ROLE_MEMBERSHIP_ASPECT_NAME,
                    roles(ADMIN_ROLE),
                    UrnUtils.getUrn(SYSTEM_ACTOR)))
            .isEmpty());
  }

  // --- PATCH resolution ---

  @Test
  public void testPatchRoleGrantDeniedWithoutManagePolicies() {
    grantPrivileges("EDIT_ENTITY");
    when(operationContext.getAuditStamp()).thenReturn(new AuditStamp().setActor(ACTOR).setTime(0L));
    final BatchItem item =
        TestPatchMCP.of(
            ACTOR,
            ROLE_MEMBERSHIP_ASPECT_NAME,
            "[{\"op\":\"add\",\"path\":\"/roles\",\"value\":[\"" + ADMIN_ROLE + "\"]}]");
    assertAuthFailure(validate(item));
  }

  @Test
  public void testPatchRoleGrantAllowedWithManagePolicies() {
    grantPrivileges(MANAGE_POLICIES);
    when(operationContext.getAuditStamp()).thenReturn(new AuditStamp().setActor(ACTOR).setTime(0L));
    final BatchItem item =
        TestPatchMCP.of(
            ACTOR,
            ROLE_MEMBERSHIP_ASPECT_NAME,
            "[{\"op\":\"add\",\"path\":\"/roles\",\"value\":[\"" + ADMIN_ROLE + "\"]}]");
    assertTrue(validate(item).isEmpty());
  }

  /** An unresolvable patch must not silently pass the privilege check. */
  @Test
  public void testUnresolvablePatchFailsClosed() {
    grantPrivileges("EDIT_ENTITY");
    assertAuthFailure(validate(unresolvablePatch()));
  }

  /**
   * Failing closed means assuming the worst payload and still running the check, not denying
   * outright - otherwise this case would be indistinguishable from an ordinary denial.
   */
  @Test
  public void testUnresolvablePatchAllowedForPrivilegedActor() {
    grantPrivileges(MANAGE_POLICIES);
    assertTrue(validate(unresolvablePatch()).isEmpty());
  }

  private BatchItem unresolvablePatch() {
    when(operationContext.getAuditStamp()).thenReturn(new AuditStamp().setActor(ACTOR).setTime(0L));
    return TestPatchMCP.of(
        ACTOR, ROLE_MEMBERSHIP_ASPECT_NAME, "[{\"op\":\"remove\",\"path\":\"/doesNotExist\"}]");
  }
}
