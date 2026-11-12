package com.linkedin.metadata.roles.sideeffects;

import static com.linkedin.metadata.Constants.ACCESS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ROLE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ROLE_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
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
import com.linkedin.metadata.aspect.batch.ChangeMCP;
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
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RoleReferenceDetachSideEffectTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final Urn ROLE_URN = UrnUtils.getUrn("urn:li:role:reader");
  private static final Urn OTHER_ROLE_URN = UrnUtils.getUrn("urn:li:role:writer");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");

  private static final AspectPluginConfig CONFIG =
      AspectPluginConfig.builder()
          .className(RoleReferenceDetachSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(List.of("DELETE"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(ROLE_ENTITY_NAME)
                      .aspectName(ROLE_KEY)
                      .build()))
          .build();

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
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(ROLE_URN);
    mcl.setEntityType(ROLE_ENTITY_NAME);
    mcl.setAspectName(ROLE_KEY);
    mcl.setChangeType(changeType);
    mcl.setSystemMetadata(new SystemMetadata());
    return TestMCL.builder()
        .changeType(changeType)
        .urn(ROLE_URN)
        .entitySpec(TEST_REGISTRY.getEntitySpec(ROLE_ENTITY_NAME))
        .aspectSpec(TEST_REGISTRY.getEntitySpec(ROLE_ENTITY_NAME).getAspectSpec(ROLE_KEY))
        .recordTemplate(new RoleKey().setId("reader"))
        .metadataChangeLog(mcl)
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build();
  }

  private List<MCPItem> run(MCLItem item) {
    RoleReferenceDetachSideEffect test = new RoleReferenceDetachSideEffect();
    test.setConfig(CONFIG);
    return test.postApply(OperationFingerprint.EMPTY, List.of(item), retrieverContext).toList();
  }

  private static Access accessAspect(Urn... roleUrns) {
    RoleAssociationArray roles = new RoleAssociationArray();
    for (Urn roleUrn : roleUrns) {
      roles.add(new RoleAssociation().setUrn(roleUrn));
    }
    return new Access().setRoles(roles);
  }

  private void searchReturns(Urn... urns) {
    SearchEntityArray hits = new SearchEntityArray();
    for (Urn urn : urns) {
      hits.add(new SearchEntity().setEntity(urn));
    }
    ScrollResult scroll = new ScrollResult();
    scroll.setEntities(hits);
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any())).thenReturn(scroll);
  }

  private void persistedAccess(Access access) {
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of(ACCESS_ASPECT_NAME, new Aspect(access.data()))));
  }

  @Test
  public void testRoleKeyDeleteRemovesOnlyThatRoleFromAccess() {
    searchReturns(DATASET_URN);
    Access stored = accessAspect(ROLE_URN, OTHER_ROLE_URN);
    persistedAccess(stored);

    List<MCPItem> output = run(roleKeyMcl(ChangeType.DELETE));

    assertEquals(output.size(), 1);
    PatchItemImpl patch = (PatchItemImpl) output.get(0);
    assertEquals(patch.getUrn(), DATASET_URN);
    assertEquals(patch.getAspectName(), ACCESS_ASPECT_NAME);

    ChangeMCP applied = patch.applyPatch(stored, mockAspectRetriever);
    assertEquals(applied.getAspect(Access.class), accessAspect(OTHER_ROLE_URN));
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
  public void testNonDeleteIsIgnored() {
    assertTrue(run(roleKeyMcl(ChangeType.UPSERT)).isEmpty());
  }
}
