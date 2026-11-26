package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.patch.builder.GlobalTagsPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * The coarse write gate and the privilege-constraint validators agree on structured property and
 * tag writes.
 *
 * <p>Both accept the entity's update privileges (Manage Documents on documents, Manage Users &amp;
 * Groups on users and groups, Create Entity when the entity does not exist yet, Delete Entity when
 * the aspect is deleted) or the aspect privilege (Edit Entity Properties, Edit Entity Tags). A
 * privilegeConstraints list narrows only the policy that contains it, and only when the action
 * names sub-resources. An actor with no matching privilege is denied by both. A form prompt checks
 * that the answer matches the prompt. An assigned user answers without this check: the value is
 * written as the system actor. A user who is not assigned is authorized as the caller and reaches
 * this decision.
 *
 * <p>Uses the real {@link AuthUtil} and entity registry; only the policy decision is faked, by a
 * session that grants a fixed privilege set, optionally limited by a privilege constraint.
 */
public class PrivilegeConstraintsWriteGateParityTest {

  private static final EntityRegistry REGISTRY = TestOperationContexts.defaultEntityRegistry();
  private static final AuditStamp AUDIT_STAMP =
      new AuditStamp().setTime(1000L).setActor(UrnUtils.getUrn("urn:li:corpuser:writer"));

  private static final Urn DOCUMENT = UrnUtils.getUrn("urn:li:document:my-doc");
  private static final Urn CORP_USER = UrnUtils.getUrn("urn:li:corpuser:some.user");
  private static final Urn CORP_GROUP = UrnUtils.getUrn("urn:li:corpGroup:some-group");
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");
  private static final Urn CHART = UrnUtils.getUrn("urn:li:chart:(looker,my_chart)");

  private static final Urn PROP_A = UrnUtils.getUrn("urn:li:structuredProperty:prop_a");
  private static final Urn PROP_B = UrnUtils.getUrn("urn:li:structuredProperty:prop_b");
  private static final TagUrn TAG_A = new TagUrn("tag_a");
  private static final TagUrn TAG_B = new TagUrn("tag_b");

  private static final String MANAGE_DOCUMENTS = "MANAGE_DOCUMENTS";
  private static final String MANAGE_USERS_AND_GROUPS =
      PoliciesConfig.MANAGE_USERS_AND_GROUPS_PRIVILEGE.getType();
  private static final String UPDATE_USERS = "UPDATE_USERS";
  private static final String UPDATE_GROUPS = "UPDATE_GROUPS";
  private static final String CREATE_ENTITY = "CREATE_ENTITY";
  private static final String DELETE_ENTITY = "DELETE_ENTITY";
  private static final String EDIT_ENTITY = PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType();
  private static final String EDIT_ENTITY_PROPERTIES =
      PoliciesConfig.EDIT_ENTITY_PROPERTIES_PRIVILEGE.getType();
  private static final String EDIT_ENTITY_TAGS =
      PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE.getType();
  private static final String VIEW_ENTITY_PAGE =
      PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE.getType();

  private enum Write {
    PROPERTIES_UPSERT,
    PROPERTIES_PATCH,
    PROPERTIES_DELETE,
    TAGS_UPSERT,
    TAGS_PATCH
  }

  private final Map<Urn, Map<String, Aspect>> stored = new HashMap<>();
  private final Map<Urn, Boolean> existing = new HashMap<>();
  private CachingAspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;
  private boolean previousRestApiAuthorization;

  @BeforeMethod
  public void setup() throws Exception {
    previousRestApiAuthorization = AuthUtil.isRestApiAuthorizationEnabled();
    setRestApiAuthorizationEnabled(true);
    stored.clear();
    existing.clear();

    aspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry()).thenReturn(REGISTRY);
    Mockito.doAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              Set<String> aspectNames = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                Map<String, Aspect> byName =
                    stored.getOrDefault(urn, Map.of()).entrySet().stream()
                        .filter(e -> aspectNames.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (!byName.isEmpty()) {
                  result.put(urn, byName);
                }
              }
              return result;
            })
        .when(aspectRetriever)
        .getLatestAspectObjects(any(OperationFingerprint.class), anySet(), anySet());
    Mockito.doAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              return urns.stream()
                  .collect(Collectors.toMap(u -> u, u -> existing.getOrDefault(u, false)));
            })
        .when(aspectRetriever)
        .entityExists(any(OperationFingerprint.class), anySet());

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(Mockito.mock(SearchRetriever.class))
            .graphRetriever(Mockito.mock(GraphRetriever.class))
            .cachingAspectRetriever(aspectRetriever)
            .build();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    setRestApiAuthorizationEnabled(previousRestApiAuthorization);
  }

  /**
   * {entity, exists, write, privileges held}. Each write is admitted by the API write gate and must
   * also pass the validator.
   */
  @DataProvider(name = "gateAdmittedWrites")
  public Object[][] gateAdmittedWrites() {
    return new Object[][] {
      // Documents: the gate accepts Manage Documents for create, update and delete.
      {DOCUMENT, true, Write.PROPERTIES_UPSERT, MANAGE_DOCUMENTS},
      {DOCUMENT, false, Write.PROPERTIES_UPSERT, MANAGE_DOCUMENTS},
      {DOCUMENT, true, Write.PROPERTIES_PATCH, MANAGE_DOCUMENTS},
      {DOCUMENT, false, Write.PROPERTIES_PATCH, MANAGE_DOCUMENTS},
      {DOCUMENT, true, Write.PROPERTIES_DELETE, MANAGE_DOCUMENTS},
      {DOCUMENT, true, Write.TAGS_UPSERT, MANAGE_DOCUMENTS},
      {DOCUMENT, true, Write.TAGS_PATCH, MANAGE_DOCUMENTS},
      {DOCUMENT, false, Write.TAGS_UPSERT, MANAGE_DOCUMENTS},
      // Users and groups: the gate does not accept Edit Entity at all, only the user/group
      // management privileges.
      {CORP_USER, true, Write.PROPERTIES_UPSERT, MANAGE_USERS_AND_GROUPS},
      {CORP_USER, true, Write.PROPERTIES_PATCH, UPDATE_USERS},
      {CORP_USER, true, Write.TAGS_PATCH, MANAGE_USERS_AND_GROUPS},
      {CORP_GROUP, true, Write.PROPERTIES_UPSERT, UPDATE_GROUPS},
      {CORP_GROUP, true, Write.TAGS_UPSERT, UPDATE_GROUPS},
      // Generic entities that do not exist yet: the gate admits Create Entity.
      {DATASET, false, Write.PROPERTIES_UPSERT, CREATE_ENTITY},
      {CHART, false, Write.PROPERTIES_UPSERT, CREATE_ENTITY},
      {DATASET, false, Write.TAGS_UPSERT, CREATE_ENTITY},
      // A patch of a missing entity is a create. Edit Entity is inside the create group, and
      // Create Entity is enough.
      {DATASET, false, Write.PROPERTIES_PATCH, EDIT_ENTITY},
      {DATASET, false, Write.TAGS_PATCH, EDIT_ENTITY},
      {DATASET, false, Write.PROPERTIES_PATCH, CREATE_ENTITY},
      // Deleting the aspect: the gate admits Delete Entity.
      {DATASET, true, Write.PROPERTIES_DELETE, DELETE_ENTITY},
    };
  }

  @Test(dataProvider = "gateAdmittedWrites")
  public void testWriteAdmittedByGateIsNotDeniedByValidator(
      Urn entity, boolean exists, Write write, String privilege) {
    BatchItem item = prepare(entity, exists, write);
    AuthorizationSession session = grants(Map.of(privilege, Set.of()));

    Assert.assertTrue(
        gateAdmits(session, item, exists),
        "precondition: the API write gate admits " + describe(entity, exists, write, privilege));
    Assert.assertEquals(
        validate(validatorFor(write), session, item),
        List.of(),
        describe(entity, exists, write, privilege) + " passed the API gate but the validator");
  }

  /**
   * An actor who holds none of the write privileges is denied, and so is an aspect privilege used
   * to create a missing entity. A form answer from a user who is not assigned writes as that
   * caller, so this denial applies to that prompted property as well.
   */
  @DataProvider(name = "noMatchingPrivilege")
  public Object[][] noMatchingPrivilege() {
    return new Object[][] {
      {DATASET, true, Write.PROPERTIES_PATCH, "NO_PRIVILEGES"},
      {DATASET, true, Write.PROPERTIES_UPSERT, "MANAGE_ENTITY_PROPERTIES"},
      {DATASET, true, Write.TAGS_UPSERT, "MANAGE_ENTITY_TAGS"},
      {DOCUMENT, true, Write.PROPERTIES_PATCH, "NO_PRIVILEGES"},
      // An aspect privilege does not create a missing entity.
      {DATASET, false, Write.PROPERTIES_UPSERT, EDIT_ENTITY_PROPERTIES},
      {DATASET, false, Write.PROPERTIES_PATCH, EDIT_ENTITY_PROPERTIES},
      {DATASET, false, Write.TAGS_UPSERT, EDIT_ENTITY_TAGS},
      {DATASET, false, Write.TAGS_PATCH, EDIT_ENTITY_TAGS},
    };
  }

  /**
   * A null or partial existence lookup must not be read as "does not exist". Create Entity would
   * then authorize a property or tag write on an entity that may already exist. An explicit false
   * is still a create.
   */
  @Test
  public void testMissingExistenceAnswerDoesNotAuthorizeCreate() {
    AuthorizationSession session = grants(Map.of(CREATE_ENTITY, Set.of()));
    BatchItem properties = prepare(DATASET, false, Write.PROPERTIES_UPSERT);
    BatchItem tags = prepare(DATASET, false, Write.TAGS_UPSERT);

    for (Map<Urn, Boolean> answer : List.<Map<Urn, Boolean>>of(Map.of(), Map.of(CHART, false))) {
      answerExistence(answer);
      Assert.assertEquals(
          validate(validatorFor(Write.PROPERTIES_UPSERT), session, properties).size(),
          1,
          "property upsert with existence " + answer);
      Assert.assertEquals(
          validate(validatorFor(Write.TAGS_UPSERT), session, tags).size(),
          1,
          "tag upsert with existence " + answer);
    }
    answerExistence(null);
    Assert.assertEquals(
        validate(validatorFor(Write.PROPERTIES_UPSERT), session, properties).size(), 1);
    Assert.assertEquals(validate(validatorFor(Write.TAGS_UPSERT), session, tags).size(), 1);

    answerExistence(Map.of(DATASET, false));
    Assert.assertEquals(
        validate(validatorFor(Write.PROPERTIES_UPSERT), session, properties), List.of());
    Assert.assertEquals(validate(validatorFor(Write.TAGS_UPSERT), session, tags), List.of());
  }

  private void answerExistence(@Nullable Map<Urn, Boolean> answer) {
    Mockito.doReturn(answer)
        .when(aspectRetriever)
        .entityExists(any(OperationFingerprint.class), anySet());
  }

  @Test(dataProvider = "noMatchingPrivilege")
  public void testWriteWithNoMatchingPrivilegeIsDenied(
      Urn entity, boolean exists, Write write, String privilege) {
    BatchItem item = prepare(entity, exists, write);
    AuthorizationSession session = grants(Map.of(privilege, Set.of()));

    Assert.assertFalse(
        gateAdmits(session, item, exists),
        describe(entity, exists, write, privilege) + " must be denied by the write gate");
    Assert.assertEquals(
        validate(validatorFor(write), session, item).size(),
        1,
        describe(entity, exists, write, privilege) + " must be denied by the validator");
  }

  @Test
  public void testUnconstrainedPolicyStillGrantsWhenAnotherPolicyListsProperties() {
    BatchItem item = prepare(DOCUMENT, true, Write.PROPERTIES_UPSERT);
    AuthorizationSession session =
        grants(Map.of(MANAGE_DOCUMENTS, Set.of(), EDIT_ENTITY_PROPERTIES, Set.of(PROP_A)));

    Assert.assertTrue(gateAdmits(session, item, true));
    Assert.assertEquals(validate(validatorFor(Write.PROPERTIES_UPSERT), session, item), List.of());
  }

  /**
   * {entity, exists, write, grants}. The actor holds an Edit privilege, but every grant of it is
   * constrained to prop_a / tag_a while the write changes prop_b / tag_b.
   */
  @DataProvider(name = "constrainedWrites")
  public Object[][] constrainedWrites() {
    return new Object[][] {
      {DATASET, true, Write.PROPERTIES_UPSERT, Map.of(EDIT_ENTITY_PROPERTIES, Set.of(PROP_A))},
      {DATASET, true, Write.PROPERTIES_PATCH, Map.of(EDIT_ENTITY_PROPERTIES, Set.of(PROP_A))},
      {DATASET, true, Write.PROPERTIES_DELETE, Map.of(EDIT_ENTITY_PROPERTIES, Set.of(PROP_A))},
      {DATASET, true, Write.PROPERTIES_UPSERT, Map.of(EDIT_ENTITY, Set.of(PROP_A))},
      {DATASET, true, Write.TAGS_PATCH, Map.of(EDIT_ENTITY_TAGS, Set.of(TAG_A))},
      {DATASET, true, Write.TAGS_UPSERT, Map.of(EDIT_ENTITY, Set.of(TAG_A))},
    };
  }

  @Test(dataProvider = "constrainedWrites")
  public void testConstrainedWriteIsDenied(
      Urn entity, boolean exists, Write write, Map<String, Set<Urn>> grants) {
    BatchItem item = prepare(entity, exists, write);

    Assert.assertEquals(
        validate(validatorFor(write), grants(grants), item).size(),
        1,
        write + " on " + entity.getEntityType() + " with " + grants + " must be denied");
  }

  /**
   * {entity, exists, write, privilege}. The validator leaves actors without any Edit privilege to
   * the write gate, so the gate has to deny these.
   */
  @DataProvider(name = "gateDenied")
  public Object[][] gateDenied() {
    return new Object[][] {
      {DOCUMENT, true, Write.PROPERTIES_UPSERT, VIEW_ENTITY_PAGE},
      {CORP_USER, true, Write.PROPERTIES_UPSERT, MANAGE_DOCUMENTS},
      // Create Entity never overwrites an existing entity.
      {DATASET, true, Write.PROPERTIES_UPSERT, CREATE_ENTITY},
      {DATASET, true, Write.TAGS_PATCH, CREATE_ENTITY},
      // Delete Entity does not grant edits.
      {DATASET, true, Write.PROPERTIES_UPSERT, DELETE_ENTITY},
    };
  }

  @Test(dataProvider = "gateDenied")
  public void testWriteWithoutEditPrivilegeIsDeniedByGate(
      Urn entity, boolean exists, Write write, String privilege) {
    BatchItem item = prepare(entity, exists, write);

    Assert.assertFalse(
        gateAdmits(grants(Map.of(privilege, Set.of())), item, exists),
        describe(entity, exists, write, privilege) + " must be denied by the API write gate");
  }

  // --- Scenario setup ---

  /**
   * Stores prop_a=old / tag_a on existing entities and builds a write that changes prop_b / adds
   * tag_b, so prop_b and tag_b are always among the changed sub-resources.
   */
  private BatchItem prepare(Urn entity, boolean exists, Write write) {
    existing.put(entity, exists);
    if (exists) {
      store(entity, STRUCTURED_PROPERTIES_ASPECT_NAME, props(PROP_A, "old", PROP_B, "old"));
      store(entity, GLOBAL_TAGS_ASPECT_NAME, tags(TAG_A));
    }
    switch (write) {
      case PROPERTIES_UPSERT:
        return upsert(entity, props(PROP_A, "old", PROP_B, "new"));
      case PROPERTIES_PATCH:
        return patch(
            new StructuredPropertiesPatchBuilder()
                .urn(entity)
                .setStringProperty(PROP_B, "new")
                .build());
      case PROPERTIES_DELETE:
        return TestMCP.builder()
            .urn(entity)
            .changeType(ChangeType.DELETE)
            .entitySpec(REGISTRY.getEntitySpec(entity.getEntityType()))
            .aspectSpec(
                REGISTRY
                    .getEntitySpec(entity.getEntityType())
                    .getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME))
            .build();
      case TAGS_UPSERT:
        return upsert(entity, tags(TAG_A, TAG_B));
      case TAGS_PATCH:
        return patch(new GlobalTagsPatchBuilder().urn(entity).addTag(TAG_B, null).build());
      default:
        throw new IllegalArgumentException(write.name());
    }
  }

  private static AbstractAspectAuthorizationValidator validatorFor(Write write) {
    List<String> ops =
        List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE", "PATCH", "DELETE");
    switch (write) {
      case TAGS_UPSERT:
      case TAGS_PATCH:
        return new TagPrivilegeConstraintsValidator()
            .setConfig(
                config(TagPrivilegeConstraintsValidator.class, GLOBAL_TAGS_ASPECT_NAME, ops));
      default:
        return new StructuredPropertyPrivilegeConstraintsValidator()
            .setConfig(
                config(
                    StructuredPropertyPrivilegeConstraintsValidator.class,
                    STRUCTURED_PROPERTIES_ASPECT_NAME,
                    ops));
    }
  }

  private static AspectPluginConfig config(Class<?> clazz, String aspectName, List<String> ops) {
    return AspectPluginConfig.builder()
        .className(clazz.getName())
        .enabled(true)
        .supportedOperations(ops)
        .supportedEntityAspectNames(
            List.of(
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName("*")
                    .aspectName(aspectName)
                    .build()))
        .build();
  }

  private List<AspectValidationException> validate(
      AbstractAspectAuthorizationValidator validator,
      AuthorizationSession session,
      BatchItem item) {
    return validator
        .validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY, List.of(item), retrieverContext, session)
        .collect(Collectors.toList());
  }

  private static boolean gateAdmits(AuthorizationSession session, BatchItem item, boolean exists) {
    return AuthUtil.isCoarseSubResourceWriteAuthorized(
        session, item.getChangeType(), item.getUrn(), item.getAspectName(), exists);
  }

  /**
   * A session holding each privilege in {@code privileges}. A non-empty value is a
   * privilegeConstraints filter: the privilege then only applies when every sub-resource is in it,
   * as {@code PolicyEngine.isSubResourceAllowed} evaluates it.
   */
  private static AuthorizationSession grants(Map<String, Set<Urn>> privileges) {
    return new AuthorizationSession() {
      @Override
      public AuthorizationResult authorize(
          @Nonnull String privilege, @Nullable EntitySpec resourceSpec) {
        return authorize(privilege, resourceSpec, List.of());
      }

      @Override
      public AuthorizationResult authorize(
          @Nonnull String privilege,
          @Nullable EntitySpec resourceSpec,
          @Nonnull Collection<EntitySpec> subResources) {
        Set<Urn> constraint = privileges.get(privilege);
        boolean wildcard =
            subResources.stream()
                .anyMatch(
                    s ->
                        com.linkedin.metadata.Constants.WILDCARD_URN
                            .toString()
                            .equals(s.getEntity()));
        boolean allowed =
            constraint != null
                && (constraint.isEmpty()
                    || wildcard
                    || (!subResources.isEmpty()
                        && subResources.stream()
                            .allMatch(s -> constraint.contains(UrnUtils.getUrn(s.getEntity())))));
        return new AuthorizationResult(
            null, allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY, null);
      }
    };
  }

  // --- Builders ---

  private void store(Urn entity, String aspectName, RecordTemplate aspect) {
    stored.computeIfAbsent(entity, u -> new HashMap<>()).put(aspectName, new Aspect(aspect.data()));
  }

  private static BatchItem upsert(Urn entity, RecordTemplate aspect) {
    return TestMCP.ofOneUpsertItem(entity, aspect, REGISTRY).iterator().next();
  }

  private static BatchItem patch(MetadataChangeProposal mcp) {
    return PatchItemImpl.builder().build(mcp, AUDIT_STAMP, REGISTRY);
  }

  private static StructuredProperties props(Urn p1, String v1, Urn p2, String v2) {
    StructuredPropertyValueAssignmentArray assignments =
        new StructuredPropertyValueAssignmentArray();
    assignments.add(assignment(p1, v1));
    assignments.add(assignment(p2, v2));
    return new StructuredProperties().setProperties(assignments);
  }

  private static StructuredPropertyValueAssignment assignment(Urn property, String value) {
    return new StructuredPropertyValueAssignment()
        .setPropertyUrn(property)
        .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(value)));
  }

  private static GlobalTags tags(TagUrn... tagUrns) {
    TagAssociationArray associations = new TagAssociationArray();
    for (TagUrn tag : tagUrns) {
      associations.add(new TagAssociation().setTag(tag));
    }
    return new GlobalTags().setTags(associations);
  }

  private static String describe(Urn entity, boolean exists, Write write, String privilege) {
    return String.format(
        "%s on %s %s with only %s",
        write, exists ? "existing" : "new", entity.getEntityType(), privilege);
  }

  private static void setRestApiAuthorizationEnabled(boolean enabled) throws Exception {
    Field field = AuthUtil.class.getDeclaredField("isRestApiAuthorizationEnabled");
    field.setAccessible(true);
    field.setBoolean(null, enabled);
  }
}
