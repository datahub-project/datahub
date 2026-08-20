package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ROLE_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
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
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.test.metadata.aspect.batch.TestPatchMCP;
import java.util.List;
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

    grantPrivileges();
  }

  /** Stubs the session to allow exactly the named privileges. */
  private void grantPrivileges(String... privileges) {
    final Set<String> granted = Set.of(privileges);
    when(session.authorize(anyString(), any()))
        .thenAnswer(
            args ->
                new AuthorizationResult(
                    null,
                    granted.contains(args.getArgument(0).toString())
                        ? AuthorizationResult.Type.ALLOW
                        : AuthorizationResult.Type.DENY,
                    null));
  }

  private void existingAspect(Urn urn, String aspectName, RecordTemplate aspect) {
    when(aspectRetriever.getLatestAspectObject(any(), any(Urn.class), anyString()))
        .thenAnswer(
            args ->
                urn.equals(args.getArgument(1)) && aspectName.equals(args.getArgument(2))
                    ? new Aspect(aspect.data())
                    : null);
  }

  private BatchItem upsert(Urn urn, String aspectName, RecordTemplate aspect, Urn actor) {
    final EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getName()).thenReturn(urn.getEntityType());
    final AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn(aspectName);

    return TestMCP.builder()
        .urn(urn)
        .changeType(ChangeType.UPSERT)
        .entitySpec(entitySpec)
        .aspectSpec(aspectSpec)
        .recordTemplate(aspect)
        .auditStamp(new AuditStamp().setActor(actor).setTime(0L))
        .build();
  }

  private List<AspectValidationException> validate(BatchItem item) {
    return validator.validateItems(
        operationContext, List.of(item), List.of(item), retrieverContext, session);
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
