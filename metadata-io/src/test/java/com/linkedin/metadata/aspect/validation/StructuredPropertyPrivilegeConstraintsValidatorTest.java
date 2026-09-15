package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StructuredPropertyPrivilegeConstraintsValidatorTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final AuditStamp AUDIT_STAMP =
      new AuditStamp().setTime(1000L).setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"));

  private StructuredPropertyPrivilegeConstraintsValidator validator;
  private SearchRetriever mockSearchRetriever;
  private CachingAspectRetriever mockAspectRetriever;
  private GraphRetriever mockGraphRetriever;
  private RetrieverContext retrieverContext;
  private AuthorizationSession mockAuthSession;
  private MockedStatic<AuthUtil> authUtilMockedStatic;

  private final Map<Urn, Map<String, Aspect>> currentAspects = new HashMap<>();

  @BeforeMethod
  public void setup() {
    authUtilMockedStatic = Mockito.mockStatic(AuthUtil.class);
    validator = new StructuredPropertyPrivilegeConstraintsValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(StructuredPropertyPrivilegeConstraintsValidator.class.getName())
            .enabled(true)
            .supportedOperations(
                List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE", "PATCH"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName(STRUCTURED_PROPERTIES_ASPECT_NAME)
                        .build()))
            .build());

    mockSearchRetriever = Mockito.mock(SearchRetriever.class);
    mockGraphRetriever = Mockito.mock(GraphRetriever.class);
    mockAspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    mockAuthSession = Mockito.mock(AuthorizationSession.class);

    currentAspects.clear();
    Mockito.doAnswer(
            invocation -> {
              Set<Urn> requestedUrns = invocation.getArgument(1);
              Set<String> requestedAspects = invocation.getArgument(2);
              Assert.assertEquals(
                  requestedUrns.stream().map(Urn::getEntityType).distinct().count(),
                  1L,
                  "getLatestAspectObjects is scoped to a single entity type: " + requestedUrns);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              requestedUrns.forEach(
                  urn -> {
                    Map<String, Aspect> byName = new HashMap<>();
                    currentAspects
                        .getOrDefault(urn, Map.of())
                        .forEach(
                            (aspectName, aspect) -> {
                              if (requestedAspects.contains(aspectName)) {
                                byName.put(aspectName, aspect);
                              }
                            });
                    if (!byName.isEmpty()) {
                      result.put(urn, byName);
                    }
                  });
              return result;
            })
        .when(mockAspectRetriever)
        .getLatestAspectObjects(any(OperationFingerprint.class), anySet(), anySet());

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(mockSearchRetriever)
            .graphRetriever(mockGraphRetriever)
            .cachingAspectRetriever(mockAspectRetriever)
            .build();
  }

  @AfterMethod
  public void tearDown() {
    if (authUtilMockedStatic != null) {
      authUtilMockedStatic.close();
    }
  }

  private void stubCurrentAspect(Urn urn, String aspectName, Aspect aspect) {
    if (aspect == null) {
      Map<String, Aspect> byName = currentAspects.get(urn);
      if (byName != null) {
        byName.remove(aspectName);
      }
      return;
    }
    currentAspects.computeIfAbsent(urn, u -> new HashMap<>()).put(aspectName, aspect);
  }

  private static StructuredProperties props(Map<String, List<String>> byUrn) {
    StructuredPropertyValueAssignmentArray arr = new StructuredPropertyValueAssignmentArray();
    byUrn.forEach(
        (propUrn, values) -> {
          PrimitivePropertyValueArray vals = new PrimitivePropertyValueArray();
          values.forEach(v -> vals.add(PrimitivePropertyValue.create(v)));
          arr.add(
              new StructuredPropertyValueAssignment()
                  .setPropertyUrn(UrnUtils.getUrn(propUrn))
                  .setValues(vals));
        });
    return new StructuredProperties().setProperties(arr);
  }

  @Test
  public void testUpsertAllowedWhenAuthorized() {
    StructuredProperties newProps = props(Map.of("urn:li:structuredProperty:p1", List.of("v1")));
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newProps, TEST_REGISTRY).stream()
            .findFirst()
            .get();
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
                    any(), any(), anyCollection()))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(item),
            retrieverContext,
            mockAuthSession);
    Assert.assertFalse(result.findAny().isPresent());
  }

  @Test
  public void testUpsertDeniedWhenUnauthorized() {
    StructuredProperties newProps = props(Map.of("urn:li:structuredProperty:p1", List.of("v1")));
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newProps, TEST_REGISTRY).stream()
            .findFirst()
            .get();
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
                    any(), any(), anyCollection()))
        .thenReturn(false);

    AspectValidationException ex =
        validator
            .validateProposedAspectsWithAuth(
                OperationFingerprint.EMPTY,
                Collections.singletonList(item),
                retrieverContext,
                mockAuthSession)
            .findFirst()
            .orElse(null);
    Assert.assertNotNull(ex);
    Assert.assertTrue(ex.getMessage().contains("structured property"));
  }

  // The key behavioral test: only added/removed/value-changed property URNs are authorized;
  // an unchanged property is NOT re-checked.
  @Test
  public void testOnlyChangedPropertiesAreAuthorized() {
    // current: p1=v1 (kept), p2=v1 (value will change)
    StructuredProperties current =
        props(
            Map.of(
                "urn:li:structuredProperty:p1", List.of("v1"),
                "urn:li:structuredProperty:p2", List.of("v1")));
    stubCurrentAspect(
        TEST_DATASET_URN, STRUCTURED_PROPERTIES_ASPECT_NAME, new Aspect(current.data()));

    // new: p1=v1 (unchanged), p2=v2 (changed), p3=v1 (added). p2+p3 must be checked, p1 must not.
    StructuredProperties updated =
        props(
            Map.of(
                "urn:li:structuredProperty:p1", List.of("v1"),
                "urn:li:structuredProperty:p2", List.of("v2"),
                "urn:li:structuredProperty:p3", List.of("v1")));
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, updated, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
                    any(), any(), anyCollection()))
        .thenReturn(true);

    validator
        .validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(item),
            retrieverContext,
            mockAuthSession)
        .forEach(e -> {});

    Set<Urn> expected =
        Set.of(
            UrnUtils.getUrn("urn:li:structuredProperty:p2"),
            UrnUtils.getUrn("urn:li:structuredProperty:p3"));
    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
                any(), eq(TEST_DATASET_URN), eq(expected)));
  }

  // PATCH writes must be enforced too, not just full-aspect UPSERT: this exercises the
  // ALTERNATE_MCP_VALIDATION path where patches arrive as ProposedItem (not PatchItemImpl).
  @Test
  public void testPatchRemoveAuthorizesRemovedProperty() {
    // current: p1=v1
    StructuredProperties current = props(Map.of("urn:li:structuredProperty:p1", List.of("v1")));
    stubCurrentAspect(
        TEST_DATASET_URN, STRUCTURED_PROPERTIES_ASPECT_NAME, new Aspect(current.data()));
    Mockito.when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);

    // PATCH removing p1 — a ProposedItem carrying a REMOVE op at /properties/<p1>
    BatchItem patchItem =
        structuredPropertiesPatchRemove(TEST_DATASET_URN, "urn:li:structuredProperty:p1");

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
                    any(), any(), anyCollection()))
        .thenReturn(true);

    validator
        .validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(patchItem),
            retrieverContext,
            mockAuthSession)
        .forEach(e -> {});

    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
                any(),
                eq(TEST_DATASET_URN),
                eq(Set.of(UrnUtils.getUrn("urn:li:structuredProperty:p1")))));
  }

  // Deleting the whole aspect must authorize every currently-assigned property, not bypass the
  // check entirely (DELETE previously wasn't dispatched to this validator at all).
  @Test
  public void testDeleteAspectAuthorizesAllCurrentProperties() {
    StructuredProperties current =
        props(
            Map.of(
                "urn:li:structuredProperty:p1", List.of("v1"),
                "urn:li:structuredProperty:p2", List.of("v2")));
    stubCurrentAspect(
        TEST_DATASET_URN, STRUCTURED_PROPERTIES_ASPECT_NAME, new Aspect(current.data()));

    BatchItem deleteItem = structuredPropertiesDelete(TEST_DATASET_URN);

    Set<Urn> expected =
        Set.of(
            UrnUtils.getUrn("urn:li:structuredProperty:p1"),
            UrnUtils.getUrn("urn:li:structuredProperty:p2"));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
                    any(), any(), anyCollection()))
        .thenReturn(true);

    validator
        .validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(deleteItem),
            retrieverContext,
            mockAuthSession)
        .forEach(e -> {});

    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
                any(), eq(TEST_DATASET_URN), eq(expected)));
  }

  @Test
  public void testDeleteAspectDeniedWhenUnauthorized() {
    StructuredProperties current = props(Map.of("urn:li:structuredProperty:p1", List.of("v1")));
    stubCurrentAspect(
        TEST_DATASET_URN, STRUCTURED_PROPERTIES_ASPECT_NAME, new Aspect(current.data()));

    BatchItem deleteItem = structuredPropertiesDelete(TEST_DATASET_URN);

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
                    any(), any(), anyCollection()))
        .thenReturn(false);

    AspectValidationException ex =
        validator
            .validateProposedAspectsWithAuth(
                OperationFingerprint.EMPTY,
                Collections.singletonList(deleteItem),
                retrieverContext,
                mockAuthSession)
            .findFirst()
            .orElse(null);
    Assert.assertNotNull(ex);
    Assert.assertTrue(ex.getMessage().contains("structured property"));
  }

  /** Builds a DELETE {@link BatchItem} removing the whole {@code structuredProperties} aspect. */
  private BatchItem structuredPropertiesDelete(Urn entityUrn) {
    return TestMCP.builder()
        .urn(entityUrn)
        .changeType(ChangeType.DELETE)
        .entitySpec(TEST_REGISTRY.getEntitySpec(entityUrn.getEntityType()))
        .aspectSpec(TEST_REGISTRY.getAspectSpecs().get(STRUCTURED_PROPERTIES_ASPECT_NAME))
        .build();
  }

  /**
   * Builds a PATCH {@link ProposedItem} (not a {@link
   * com.linkedin.metadata.entity.ebean.batch.PatchItemImpl}) removing a structured property, the
   * same shape {@link com.linkedin.metadata.aspect.patch.builder.StructuredPropertiesPatchBuilder}
   * emits: {@code {"op":"remove","path":"/properties/<propertyUrn>"}}.
   */
  private BatchItem structuredPropertiesPatchRemove(Urn entityUrn, String propertyUrn) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(entityUrn);
    mcp.setEntityType(entityUrn.getEntityType());
    mcp.setAspectName(STRUCTURED_PROPERTIES_ASPECT_NAME);
    mcp.setChangeType(ChangeType.PATCH);

    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("remove");
    patchOp.setPath("/properties/" + propertyUrn);

    Map<String, List<String>> arrayPrimaryKeys = new HashMap<>();
    arrayPrimaryKeys.put("properties", List.of("propertyUrn", "attribution␟source"));

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp))
            .arrayPrimaryKeys(arrayPrimaryKeys)
            .build();
    mcp.setAspect(GenericRecordUtils.serializePatch(genericJsonPatch, OBJECT_MAPPER));

    return ProposedItem.builder().build(mcp, AUDIT_STAMP, TEST_REGISTRY);
  }
}
