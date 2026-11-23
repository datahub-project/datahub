package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.WILDCARD_URN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * The ingest gate for aspects whose validator checks sub-resources (structured properties, tags)
 * picks create, update or delete from the change type the same way the validators do. Uses the real
 * {@link AuthUtil}; only the policy decision is faked.
 */
public class EntityAuthorizationUtilsSubResourceGateTest {

  private static final EntityRegistry REGISTRY = TestOperationContexts.defaultEntityRegistry();
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");
  private static final Urn DOCUMENT = UrnUtils.getUrn("urn:li:document:my-doc");
  private static final Urn PROP_A = UrnUtils.getUrn("urn:li:structuredProperty:prop_a");

  private static final Urn TAG_A = UrnUtils.getUrn("urn:li:tag:tag_a");

  private static final String EDIT_ENTITY = PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType();
  private static final String EDIT_ENTITY_TAGS =
      PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE.getType();
  private static final String EDIT_DATASET_COL_TAGS =
      PoliciesConfig.EDIT_DATASET_COL_TAGS_PRIVILEGE.getType();
  private static final String EDIT_ENTITY_PROPERTIES =
      PoliciesConfig.EDIT_ENTITY_PROPERTIES_PRIVILEGE.getType();
  private static final String CREATE_ENTITY = PoliciesConfig.CREATE_ENTITY_PRIVILEGE.getType();
  private static final String DELETE_ENTITY = PoliciesConfig.DELETE_ENTITY_PRIVILEGE.getType();

  private boolean previousRestApiAuthorization;

  @BeforeMethod
  public void setup() throws Exception {
    previousRestApiAuthorization = AuthUtil.isRestApiAuthorizationEnabled();
    setRestApiAuthorizationEnabled(true);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    setRestApiAuthorizationEnabled(previousRestApiAuthorization);
  }

  /** {urn, exists, changeType, aspect, privilege, privilegeConstraints, expected status}. */
  @DataProvider(name = "gateCases")
  public Object[][] gateCases() {
    return new Object[][] {
      // Deleting the aspect is a delete, so Delete Entity is enough.
      {
        DATASET,
        true,
        ChangeType.DELETE,
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        DELETE_ENTITY,
        Set.of(),
        HttpStatus.SC_OK
      },
      {
        DATASET,
        true,
        ChangeType.DELETE,
        GLOBAL_TAGS_ASPECT_NAME,
        DELETE_ENTITY,
        Set.of(),
        HttpStatus.SC_OK
      },
      // A patch of a missing entity is a create. Edit Entity is inside the create group.
      {
        DATASET,
        false,
        ChangeType.PATCH,
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        EDIT_ENTITY,
        Set.of(),
        HttpStatus.SC_OK
      },
      // An aspect privilege does not create the entity.
      {
        DATASET,
        false,
        ChangeType.UPSERT,
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        EDIT_ENTITY_PROPERTIES,
        Set.of(),
        HttpStatus.SC_FORBIDDEN
      },
      {
        DATASET,
        false,
        ChangeType.PATCH,
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        EDIT_ENTITY_PROPERTIES,
        Set.of(),
        HttpStatus.SC_FORBIDDEN
      },
      {
        DATASET,
        false,
        ChangeType.UPSERT,
        GLOBAL_TAGS_ASPECT_NAME,
        EDIT_ENTITY_TAGS,
        Set.of(),
        HttpStatus.SC_FORBIDDEN
      },
      {
        DATASET,
        true,
        ChangeType.PATCH,
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        CREATE_ENTITY,
        Set.of(),
        HttpStatus.SC_FORBIDDEN
      },
      // A property list is checked by the validator; the gate lets it through.
      {
        DATASET,
        true,
        ChangeType.UPSERT,
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        EDIT_ENTITY_PROPERTIES,
        Set.of(PROP_A),
        HttpStatus.SC_OK
      },
      // A schema aspect holds field types, descriptions and terms as well as tags, so a column tag
      // privilege does not admit a write to it. The validator limits the tag changes afterwards.
      {
        DATASET,
        true,
        ChangeType.UPSERT,
        SCHEMA_METADATA_ASPECT_NAME,
        EDIT_DATASET_COL_TAGS,
        Set.of(),
        HttpStatus.SC_FORBIDDEN
      },
      {
        DATASET,
        true,
        ChangeType.UPSERT,
        EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        EDIT_DATASET_COL_TAGS,
        Set.of(),
        HttpStatus.SC_FORBIDDEN
      },
      {
        DATASET,
        true,
        ChangeType.PATCH,
        EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        EDIT_ENTITY_TAGS,
        Set.of(),
        HttpStatus.SC_FORBIDDEN
      },
      {
        DATASET,
        true,
        ChangeType.PATCH,
        EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        EDIT_ENTITY,
        Set.of(),
        HttpStatus.SC_OK
      },
      // Deleting a schema aspect is an entity delete. A tag privilege does not grant it.
      {
        DATASET,
        true,
        ChangeType.DELETE,
        SCHEMA_METADATA_ASPECT_NAME,
        EDIT_ENTITY_TAGS,
        Set.of(),
        HttpStatus.SC_FORBIDDEN
      },
      {
        DATASET,
        true,
        ChangeType.DELETE,
        EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        EDIT_DATASET_COL_TAGS,
        Set.of(),
        HttpStatus.SC_FORBIDDEN
      },
      {
        DATASET,
        true,
        ChangeType.DELETE,
        SCHEMA_METADATA_ASPECT_NAME,
        DELETE_ENTITY,
        Set.of(),
        HttpStatus.SC_OK
      },
      {
        DATASET,
        true,
        ChangeType.DELETE,
        EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        DELETE_ENTITY,
        Set.of(),
        HttpStatus.SC_OK
      },
      // A tag list on Edit Entity names sub-resources only when tags change; the gate passes it and
      // the validator applies the list to the changed tags.
      {
        DATASET,
        true,
        ChangeType.UPSERT,
        SCHEMA_METADATA_ASPECT_NAME,
        EDIT_ENTITY,
        Set.of(TAG_A),
        HttpStatus.SC_OK
      },
      // Other aspects keep the entity privilege check, which a property grant does not satisfy.
      {
        DOCUMENT,
        true,
        ChangeType.UPSERT,
        DOCUMENT_INFO_ASPECT_NAME,
        EDIT_ENTITY_PROPERTIES,
        Set.of(PROP_A),
        HttpStatus.SC_FORBIDDEN
      },
    };
  }

  /**
   * The gate's existence stub normally fills every requested URN. These answers do not: an empty
   * map, a map that names a different entity, and a null map. Create Entity must not be granted off
   * any of them. An explicit false is still a create.
   */
  @Test
  public void testMissingExistenceAnswerDoesNotGrantCreate() {
    MetadataChangeProposal properties =
        proposal(DATASET, ChangeType.UPSERT, STRUCTURED_PROPERTIES_ASPECT_NAME);
    MetadataChangeProposal tags = proposal(DATASET, ChangeType.UPSERT, GLOBAL_TAGS_ASPECT_NAME);
    Map<String, Set<Urn>> createOnly = Map.of(CREATE_ENTITY, Set.of());

    for (Map<Urn, Boolean> answer : List.<Map<Urn, Boolean>>of(Map.of(), Map.of(DOCUMENT, false))) {
      assertEquals(
          ingestStatus(properties, createOnly, answer),
          HttpStatus.SC_FORBIDDEN,
          String.valueOf(answer));
      assertEquals(
          ingestStatus(tags, createOnly, answer), HttpStatus.SC_FORBIDDEN, String.valueOf(answer));
    }
    assertEquals(ingestStatus(properties, createOnly, null), HttpStatus.SC_FORBIDDEN);
    assertEquals(ingestStatus(tags, createOnly, null), HttpStatus.SC_FORBIDDEN);

    assertEquals(ingestStatus(properties, createOnly, Map.of(DATASET, false)), HttpStatus.SC_OK);
    assertEquals(ingestStatus(tags, createOnly, Map.of(DATASET, false)), HttpStatus.SC_OK);
  }

  private static MetadataChangeProposal proposal(
      Urn urn, ChangeType changeType, String aspectName) {
    return new MetadataChangeProposal()
        .setEntityType(urn.getEntityType())
        .setEntityUrn(urn)
        .setChangeType(changeType)
        .setAspectName(aspectName);
  }

  private static int ingestStatus(
      MetadataChangeProposal proposal,
      Map<String, Set<Urn>> privileges,
      Map<Urn, Boolean> existence) {
    OperationContext opContext = session(privileges, Map.of());
    when(opContext.getAspectRetriever().entityExists(any(OperationFingerprint.class), anySet()))
        .thenReturn(existence);
    return EntityAuthorizationUtils.isAPIAuthorizedIngest(opContext, REGISTRY, List.of(proposal))
        .get(0)
        .getSecond();
  }

  @Test(dataProvider = "gateCases")
  public void testIngestGate(
      Urn urn,
      boolean exists,
      ChangeType changeType,
      String aspectName,
      String privilege,
      Set<Urn> privilegeConstraints,
      int expected) {
    MetadataChangeProposal proposal =
        new MetadataChangeProposal()
            .setEntityType(urn.getEntityType())
            .setEntityUrn(urn)
            .setChangeType(changeType)
            .setAspectName(aspectName);
    OperationContext opContext =
        session(Map.of(privilege, privilegeConstraints), Map.of(urn, exists));

    List<Pair<MetadataChangeProposal, Integer>> result =
        EntityAuthorizationUtils.isAPIAuthorizedIngest(opContext, REGISTRY, List.of(proposal));

    assertEquals(
        result.get(0).getSecond().intValue(),
        expected,
        String.format(
            "%s %s on %s %s with %s%s",
            changeType,
            aspectName,
            exists ? "existing" : "new",
            urn.getEntityType(),
            privilege,
            privilegeConstraints));
  }

  /**
   * An operation context that holds each privilege in {@code privileges}. A non-empty value is a
   * privilegeConstraints list: the privilege then applies to a wildcard or listed sub-resource, and
   * to an action that names none, as {@code PolicyEngine.isSubResourceAllowed} evaluates it.
   */
  private static OperationContext session(
      Map<String, Set<Urn>> privileges, Map<Urn, Boolean> existing) {
    OperationContext opContext = mock(OperationContext.class);
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    when(aspectRetriever.entityExists(any(OperationFingerprint.class), anySet()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              return urns.stream()
                  .collect(Collectors.toMap(u -> u, u -> existing.getOrDefault(u, false)));
            });
    when(opContext.authorize(anyString(), any()))
        .thenAnswer(invocation -> decide(privileges, invocation.getArgument(0), List.of()));
    when(opContext.authorize(anyString(), any(), anyCollection()))
        .thenAnswer(
            invocation -> decide(privileges, invocation.getArgument(0), invocation.getArgument(2)));
    return opContext;
  }

  private static AuthorizationResult decide(
      Map<String, Set<Urn>> privileges, String privilege, Collection<EntitySpec> subResources) {
    Set<Urn> constraint = privileges.get(privilege);
    boolean allowed =
        constraint != null
            && (constraint.isEmpty()
                || subResources.isEmpty()
                || subResources.stream()
                    .allMatch(
                        s ->
                            WILDCARD_URN.toString().equals(s.getEntity())
                                || constraint.contains(UrnUtils.getUrn(s.getEntity()))));
    return new AuthorizationResult(
        null, allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY, null);
  }

  private static void setRestApiAuthorizationEnabled(boolean enabled) throws Exception {
    Field field = AuthUtil.class.getDeclaredField("isRestApiAuthorizationEnabled");
    field.setAccessible(true);
    field.setBoolean(null, enabled);
  }
}
