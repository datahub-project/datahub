package com.linkedin.metadata.roles.sideeffects;

import static com.linkedin.metadata.Constants.ACCESS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ROLE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ROLE_KEY;
import static com.linkedin.metadata.utils.metrics.CascadeOperationContext.SYSTEM_METADATA_CASCADE_ID_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Access;
import com.linkedin.common.RoleAssociation;
import com.linkedin.common.RoleAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.key.RoleKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RoleReferenceDetachSideEffectTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final Urn ROLE_URN = UrnUtils.getUrn("urn:li:role:reader");
  private static final Urn OTHER_ROLE_URN = UrnUtils.getUrn("urn:li:role:writer");
  private static final Urn ARN_ROLE_URN =
      UrnUtils.getUrn("urn:li:role:arn:aws:iam::123456789012:role/data/Admin");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
  private static final Urn OTHER_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.other,PROD)");
  // Fails URN validation when its patch is built.
  private static final Urn INVALID_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.bad\u241ftable,PROD)");

  private CachingAspectRetriever mockAspectRetriever;
  private SearchRetriever mockSearchRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(CachingAspectRetriever.class);
    mockSearchRetriever = mock(SearchRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mockSearchRetriever)
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
  }

  private MCLItem roleKeyMcl(ChangeType changeType) {
    return roleKeyMcl(ROLE_URN, changeType);
  }

  private MCLItem roleKeyMcl(Urn roleUrn, ChangeType changeType) {
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(roleUrn);
    mcl.setEntityType(ROLE_ENTITY_NAME);
    mcl.setAspectName(ROLE_KEY);
    mcl.setChangeType(changeType);
    mcl.setSystemMetadata(new SystemMetadata());
    return TestMCL.builder()
        .changeType(changeType)
        .urn(roleUrn)
        .entitySpec(TEST_REGISTRY.getEntitySpec(ROLE_ENTITY_NAME))
        .aspectSpec(TEST_REGISTRY.getEntitySpec(ROLE_ENTITY_NAME).getAspectSpec(ROLE_KEY))
        .recordTemplate(new RoleKey().setId(roleUrn.getId()))
        .metadataChangeLog(mcl)
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build();
  }

  private List<MCPItem> run(MCLItem item) {
    RoleReferenceDetachSideEffect sideEffect = new RoleReferenceDetachSideEffect();
    sideEffect.setConfig(sideEffect.keyDeleteConfig());
    return run(sideEffect, item);
  }

  private List<MCPItem> run(RoleReferenceDetachSideEffect sideEffect, MCLItem item) {
    return sideEffect
        .postApply(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
        .toList();
  }

  private static Access accessAspect(Urn... roleUrns) {
    RoleAssociationArray roles = new RoleAssociationArray();
    for (Urn roleUrn : roleUrns) {
      roles.add(new RoleAssociation().setUrn(roleUrn));
    }
    return new Access().setRoles(roles);
  }

  private static ScrollResult page(String scrollId, Urn... urns) {
    SearchEntityArray hits = new SearchEntityArray();
    for (Urn urn : urns) {
      hits.add(new SearchEntity().setEntity(urn));
    }
    ScrollResult scroll = new ScrollResult();
    scroll.setEntities(hits);
    if (scrollId != null) {
      scroll.setScrollId(scrollId);
    }
    return scroll;
  }

  private void searchReturns(Urn... urns) {
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any()))
        .thenReturn(page(null, urns));
  }

  private void persistedAccess(Access access) {
    persistedAccess(Map.of(DATASET_URN, access));
  }

  private void persistedAccess(Map<Urn, Access> accessByUrn) {
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(
            accessByUrn.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        e -> Map.of(ACCESS_ASPECT_NAME, new Aspect(e.getValue().data())))));
  }

  private static Set<Urn> patchedUrns(List<MCPItem> output) {
    return output.stream().map(MCPItem::getUrn).collect(Collectors.toSet());
  }

  private Access applied(MCPItem item, Access stored) {
    PatchItemImpl patch =
        PatchItemImpl.builder()
            .build(
                ((PatchItemImpl) item).getMetadataChangeProposal(),
                AuditStampUtils.createDefaultAuditStamp(),
                TEST_REGISTRY);
    return patch.applyPatch(stored, mockAspectRetriever).getAspect(Access.class);
  }

  @Test
  public void testRoleKeyDeleteRemovesOnlyThatRoleFromAccess() {
    searchReturns(DATASET_URN);
    Access stored = accessAspect(ROLE_URN, OTHER_ROLE_URN);
    persistedAccess(stored);

    List<MCPItem> output = run(roleKeyMcl(ChangeType.DELETE));

    assertEquals(output.size(), 1);
    assertEquals(output.get(0).getUrn(), DATASET_URN);
    assertEquals(output.get(0).getAspectName(), ACCESS_ASPECT_NAME);
    assertEquals(applied(output.get(0), stored), accessAspect(OTHER_ROLE_URN));
  }

  @Test
  public void testPatchesCarryCascadeIdWithoutTaggingTheDeleteEvent() {
    searchReturns(DATASET_URN, OTHER_DATASET_URN);
    persistedAccess(
        Map.of(DATASET_URN, accessAspect(ROLE_URN), OTHER_DATASET_URN, accessAspect(ROLE_URN)));
    MCLItem roleDelete = roleKeyMcl(ChangeType.DELETE);

    List<MCPItem> output = run(roleDelete);

    assertEquals(output.size(), 2);
    Set<String> cascadeIds =
        output.stream()
            .map(
                item ->
                    item.getSystemMetadata().getProperties().get(SYSTEM_METADATA_CASCADE_ID_KEY))
            .collect(Collectors.toSet());
    assertEquals(cascadeIds.size(), 1);
    assertNotNull(cascadeIds.iterator().next());
    assertFalse(
        roleDelete.getSystemMetadata().hasProperties()
            && roleDelete
                .getSystemMetadata()
                .getProperties()
                .containsKey(SYSTEM_METADATA_CASCADE_ID_KEY));
  }

  @Test
  public void testSearchesRolesFieldOnEntitiesWithAccessAspect() {
    searchReturns();

    run(roleKeyMcl(ChangeType.DELETE));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<String>> entities = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<Filter> filter = ArgumentCaptor.forClass(Filter.class);
    verify(mockSearchRetriever)
        .scroll(entities.capture(), filter.capture(), any(), any(), eq(List.of()), any());
    assertTrue(entities.getValue().contains(DATASET_ENTITY_NAME));
    assertEquals(
        filter.getValue().getOr().get(0).getAnd().get(0).getField(),
        RoleReferenceDetachSideEffect.ROLES_SEARCH_FIELD);
  }

  @Test
  public void testStaleSearchHitProducesNoPatch() {
    searchReturns(DATASET_URN);
    persistedAccess(accessAspect(OTHER_ROLE_URN));

    assertTrue(run(roleKeyMcl(ChangeType.DELETE)).isEmpty());
  }

  @Test
  public void testNonDeleteIsIgnoredEvenWhenConfigAllowsIt() {
    RoleReferenceDetachSideEffect sideEffect = new RoleReferenceDetachSideEffect();
    sideEffect.setConfig(
        AspectPluginConfig.builder()
            .className(RoleReferenceDetachSideEffect.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT", "DELETE"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(ROLE_ENTITY_NAME)
                        .aspectName(ROLE_KEY)
                        .build()))
            .build());

    assertTrue(run(sideEffect, roleKeyMcl(ChangeType.UPSERT)).isEmpty());
    verifyNoInteractions(mockSearchRetriever);
  }

  @Test
  public void testRoleUrnWithSlashesIsRemoved() {
    searchReturns(DATASET_URN);
    Access stored = accessAspect(ARN_ROLE_URN, OTHER_ROLE_URN);
    persistedAccess(stored);

    List<MCPItem> output = run(roleKeyMcl(ARN_ROLE_URN, ChangeType.DELETE));

    assertEquals(output.size(), 1);
    assertEquals(applied(output.get(0), stored), accessAspect(OTHER_ROLE_URN));
  }

  @Test
  public void testDetachesReferencesOnEveryPage() {
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any()))
        .thenReturn(page("next", DATASET_URN))
        .thenReturn(page(null, OTHER_DATASET_URN));
    persistedAccess(
        Map.of(DATASET_URN, accessAspect(ROLE_URN), OTHER_DATASET_URN, accessAspect(ROLE_URN)));
    RoleReferenceDetachSideEffect sideEffect = new RoleReferenceDetachSideEffect();
    sideEffect.setConfig(sideEffect.keyDeleteConfig());
    sideEffect.setPageSize(1);

    List<MCPItem> output = run(sideEffect, roleKeyMcl(ChangeType.DELETE));

    assertEquals(patchedUrns(output), Set.of(DATASET_URN, OTHER_DATASET_URN));
    verify(mockSearchRetriever).scroll(any(), any(), eq("next"), eq(1), any(), any());
  }

  @Test
  public void testAssetThatFailsIsSkippedAndOthersArePatched() {
    searchReturns(INVALID_DATASET_URN, DATASET_URN);
    persistedAccess(
        Map.of(INVALID_DATASET_URN, accessAspect(ROLE_URN), DATASET_URN, accessAspect(ROLE_URN)));

    List<MCPItem> output = run(roleKeyMcl(ChangeType.DELETE));

    assertEquals(patchedUrns(output), Set.of(DATASET_URN));
  }
}
