package com.linkedin.metadata.graph.cache.service;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.UI_SOURCE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.domain.DomainProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationBatch;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class EntityGraphSyncInvalidationSupportTest {

  @Test
  public void compositeDedupKeysAreDistinctForSameUrnAndDifferentAspects() {
    String urn = "urn:li:domain:abc";
    String domainPropertiesKey =
        EntityGraphSyncInvalidationSupport.compositeKey(urn, "domainProperties");
    String ambiguousAspectNameKey =
        EntityGraphSyncInvalidationSupport.compositeKey(urn, "domainProperties|extra");

    assertTrue(!domainPropertiesKey.equals(ambiguousAspectNameKey));
    assertTrue(domainPropertiesKey.startsWith(urn));
    assertTrue(ambiguousAspectNameKey.startsWith(urn));
  }

  @Test
  public void fromSyncEntityDeleteKeyAspectUsesEntityWideInvalidation() {
    OperationContext opContext = operationContextWithDomainGraph();

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncEntityDelete(
            opContext, "urn:li:domain:child", "domain", "domainKey", true);

    assertEquals(batch.getDeletes().size(), 1);
    assertNull(batch.getDeletes().iterator().next().getAspectName());
  }

  @Test
  public void fromSyncEntityDeleteRelationshipAspectKeepsAspectName() {
    OperationContext opContext = operationContextWithDomainGraph();

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncEntityDelete(
            opContext, "urn:li:domain:child", "domain", "domainProperties", false);

    assertEquals(batch.getDeletes().size(), 1);
    assertEquals(batch.getDeletes().iterator().next().getAspectName(), "domainProperties");
  }

  @Test
  public void fromSyncEntityDeleteSkipsWhenAspectNotInGraphConfig() {
    OperationContext opContext = operationContextWithDomainGraph();

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncEntityDelete(
            opContext, "urn:li:domain:child", "domain", "institutionalMemory", false);

    assertTrue(batch.isEmpty());
  }

  @Test
  public void fromSyncEntityDeleteSkipsEntityTypeNotInGraphConfig() {
    OperationContext opContext = operationContextWithDomainGraph();

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncEntityDelete(
            opContext, "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)", "dataset", null, true);

    assertTrue(batch.isEmpty());
  }

  @Test
  public void fromSyncMetadataChangeLogKeyAspectDeleteUsesEntityWideInvalidation() {
    OperationContext opContext = operationContextWithDomainGraph();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn entityUrn = UrnUtils.getUrn("urn:li:domain:child");
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(entityUrn);
    mcl.setEntityType("domain");
    mcl.setAspectName(opContext.getKeyAspectName(entityUrn));
    mcl.setChangeType(ChangeType.DELETE);
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    mcl.setSystemMetadata(systemMetadata);

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncMetadataChangeLog(
            opContext, preProcessHooks, mcl);

    assertEquals(batch.getDeletes().size(), 1);
    assertNull(batch.getDeletes().iterator().next().getAspectName());
  }

  @Test
  public void fromSyncIngestBatchDeleteRelationshipAspectInGraphConfig() {
    OperationContext opContext = operationContextWithDomainGraph();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:child");
    AspectsBatch aspectsBatch = syncAspectsBatch(domainUrn, "domainProperties", ChangeType.DELETE);
    UpdateAspectResult deleteResult =
        materialDeleteResult(domainUrn, "domainProperties", ChangeType.DELETE);

    SyncGraphInvalidationBatch invalidation =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, aspectsBatch, List.of(deleteResult));

    assertEquals(invalidation.getDeletes().size(), 1);
    assertEquals(invalidation.getDeletes().iterator().next().getAspectName(), "domainProperties");
  }

  @Test
  public void fromSyncIngestBatchDeleteSkipsUnconfiguredAspect() {
    OperationContext opContext = operationContextWithDomainGraph();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:child");
    AspectsBatch aspectsBatch =
        syncAspectsBatch(domainUrn, "institutionalMemory", ChangeType.DELETE);
    UpdateAspectResult deleteResult =
        materialDeleteResult(domainUrn, "institutionalMemory", ChangeType.DELETE);

    SyncGraphInvalidationBatch invalidation =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, aspectsBatch, List.of(deleteResult));

    assertTrue(invalidation.isEmpty());
  }

  @Test
  public void fromSyncIngestBatchDeleteKeyAspectInGraphConfig() {
    OperationContext opContext = operationContextWithDomainGraph();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:child");
    String keyAspectName = opContext.getKeyAspectName(domainUrn);
    AspectsBatch aspectsBatch = syncAspectsBatch(domainUrn, keyAspectName, ChangeType.DELETE);
    UpdateAspectResult deleteResult =
        materialDeleteResult(domainUrn, keyAspectName, ChangeType.DELETE);

    SyncGraphInvalidationBatch invalidation =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, aspectsBatch, List.of(deleteResult));

    assertEquals(invalidation.getDeletes().size(), 1);
    assertNull(invalidation.getDeletes().iterator().next().getAspectName());
  }

  @Test
  public void fromSyncIngestBatchDeleteSkipsUnconfiguredEntityType() {
    OperationContext opContext = operationContextWithDomainGraph();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
    AspectsBatch aspectsBatch = syncAspectsBatch(datasetUrn, "datasetKey", ChangeType.DELETE);
    UpdateAspectResult deleteResult =
        materialDeleteResult(datasetUrn, "datasetKey", ChangeType.DELETE);

    SyncGraphInvalidationBatch invalidation =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, aspectsBatch, List.of(deleteResult));

    assertTrue(invalidation.isEmpty());
  }

  @Test
  public void fromSyncIngestBatchSkipsNoOpUpdateResult() {
    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:child");
    DomainProperties aspect = new DomainProperties().setName("child");
    BatchItem item = syncBatchItem(domainUrn, "domainProperties", aspect);
    AspectsBatch batch = Mockito.mock(AspectsBatch.class);
    Mockito.doReturn(List.of(item)).when(batch).getItems();

    ChangeMCP request = Mockito.mock(ChangeMCP.class);
    Mockito.when(request.getAspectName()).thenReturn("domainProperties");
    Mockito.when(request.getChangeType()).thenReturn(ChangeType.UPSERT);
    SystemMetadata noOpSystemMetadata = new SystemMetadata();
    SystemMetadataUtils.setNoOp(noOpSystemMetadata, true);
    UpdateAspectResult noOpResult =
        UpdateAspectResult.builder()
            .urn(domainUrn)
            .request(request)
            .oldValue(aspect)
            .newValue(aspect)
            .newSystemMetadata(noOpSystemMetadata)
            .build();

    SyncGraphInvalidationBatch invalidation =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, batch, List.of(noOpResult));

    assertTrue(invalidation.isEmpty());
  }

  @Test
  public void fromSyncIngestBatchIncludesMaterialUpdateResult() {
    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:child");
    DomainProperties oldAspect = new DomainProperties().setName("child");
    DomainProperties newAspect = new DomainProperties().setName("renamed");
    BatchItem item = syncBatchItem(domainUrn, "domainProperties", newAspect);
    AspectsBatch batch = Mockito.mock(AspectsBatch.class);
    Mockito.doReturn(List.of(item)).when(batch).getItems();

    ChangeMCP request = Mockito.mock(ChangeMCP.class);
    Mockito.when(request.getAspectName()).thenReturn("domainProperties");
    Mockito.when(request.getChangeType()).thenReturn(ChangeType.UPSERT);
    UpdateAspectResult materialResult =
        UpdateAspectResult.builder()
            .urn(domainUrn)
            .request(request)
            .oldValue(oldAspect)
            .newValue(newAspect)
            .newSystemMetadata(new SystemMetadata())
            .build();

    SyncGraphInvalidationBatch invalidation =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, batch, List.of(materialResult));

    assertEquals(invalidation.getUpdates().size(), 1);
    assertEquals(invalidation.getUpdates().iterator().next().getAspectName(), "domainProperties");
  }

  @Test
  public void fromSyncMetadataChangeLogDoesNotSkipRestateWithoutPrimaryNoOpFlag() {
    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn entityUrn = UrnUtils.getUrn("urn:li:domain:child");
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(entityUrn);
    mcl.setEntityType("domain");
    mcl.setAspectName("domainProperties");
    mcl.setChangeType(ChangeType.RESTATE);
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    mcl.setSystemMetadata(systemMetadata);

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncMetadataChangeLog(
            opContext, preProcessHooks, mcl);

    assertEquals(batch.getUpdates().size(), 1);
  }

  @Test
  public void fromSyncMetadataChangeLogSkipsSystemMetadataNoOpFlag() {
    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn entityUrn = UrnUtils.getUrn("urn:li:domain:child");
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(entityUrn);
    mcl.setEntityType("domain");
    mcl.setAspectName("domainProperties");
    mcl.setChangeType(ChangeType.UPSERT);
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    SystemMetadataUtils.setNoOp(systemMetadata, true);
    mcl.setSystemMetadata(systemMetadata);

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncMetadataChangeLog(
            opContext, preProcessHooks, mcl);

    assertTrue(batch.isEmpty());
  }

  @Test
  public void fromSyncIngestBatchSkipsDomainPropertiesWithoutUiSource() {
    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:child");
    BatchItem item = Mockito.mock(BatchItem.class);
    Mockito.when(item.getUrn()).thenReturn(domainUrn);
    Mockito.when(item.getAspectName()).thenReturn("domainProperties");
    Mockito.when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    Mockito.when(item.getSystemMetadata()).thenReturn(null);
    AspectsBatch batch = Mockito.mock(AspectsBatch.class);
    Mockito.doReturn(List.of(item)).when(batch).getItems();

    SyncGraphInvalidationBatch invalidation =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, batch, Collections.emptyList());

    assertTrue(invalidation.isEmpty());
  }

  @Test
  public void fromSyncIngestBatchMixedBatchOnlyInvalidatesSyncGatedItems() {
    OperationContext opContext = operationContextWithDomainGraph();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:child");
    BatchItem syncItem = syncBatchItem(domainUrn, "domainProperties", ChangeType.UPSERT);
    BatchItem asyncItem = Mockito.mock(BatchItem.class);
    Mockito.when(asyncItem.getUrn()).thenReturn(domainUrn);
    Mockito.when(asyncItem.getAspectName()).thenReturn("ownership");
    Mockito.when(asyncItem.getChangeType()).thenReturn(ChangeType.UPSERT);
    Mockito.when(asyncItem.getSystemMetadata()).thenReturn(null);

    AspectsBatch batch = Mockito.mock(AspectsBatch.class);
    Mockito.doReturn(List.of(syncItem, asyncItem)).when(batch).getItems();

    ChangeMCP syncRequest = Mockito.mock(ChangeMCP.class);
    Mockito.when(syncRequest.getAspectName()).thenReturn("domainProperties");
    Mockito.when(syncRequest.getChangeType()).thenReturn(ChangeType.UPSERT);
    UpdateAspectResult syncResult =
        UpdateAspectResult.builder()
            .urn(domainUrn)
            .request(syncRequest)
            .oldValue(new DomainProperties().setName("child"))
            .newValue(new DomainProperties().setName("renamed"))
            .newSystemMetadata(new SystemMetadata())
            .build();

    ChangeMCP asyncRequest = Mockito.mock(ChangeMCP.class);
    Mockito.when(asyncRequest.getAspectName()).thenReturn("ownership");
    Mockito.when(asyncRequest.getChangeType()).thenReturn(ChangeType.UPSERT);
    UpdateAspectResult asyncResult =
        UpdateAspectResult.builder()
            .urn(domainUrn)
            .request(asyncRequest)
            .newSystemMetadata(new SystemMetadata())
            .build();

    SyncGraphInvalidationBatch invalidation =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, batch, List.of(syncResult, asyncResult));

    assertEquals(invalidation.getUpdates().size(), 1);
    assertEquals(invalidation.getUpdates().iterator().next().getAspectName(), "domainProperties");
  }

  @Test
  public void fromSyncAspectRollbackBuildsUpdateForConfiguredRelationshipAspect() {
    OperationContext opContext = operationContextWithDomainGraph();

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncAspectRollback(
            opContext, "urn:li:domain:child", "domain", "domainProperties");

    assertEquals(batch.getUpdates().size(), 1);
    assertEquals(batch.getUpdates().iterator().next().getAspectName(), "domainProperties");
  }

  @Test
  public void fromSyncAspectRollbackSkipsUnconfiguredAspect() {
    OperationContext opContext = operationContextWithDomainGraph();

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncAspectRollback(
            opContext, "urn:li:domain:child", "domain", "institutionalMemory");

    assertTrue(batch.isEmpty());
  }

  @Test
  public void fromSyncMetadataChangeLogIncludesNativeGroupMembershipUiUpdate() {
    OperationContext opContext = operationContextWithMembershipGraph();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:datahub");
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(userUrn);
    mcl.setEntityType("corpuser");
    mcl.setAspectName("nativeGroupMembership");
    mcl.setChangeType(ChangeType.UPSERT);
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    mcl.setSystemMetadata(systemMetadata);

    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncMetadataChangeLog(
            opContext, preProcessHooks, mcl);

    assertEquals(batch.getUpdates().size(), 1);
    assertEquals(batch.getUpdates().iterator().next().getAspectName(), "nativeGroupMembership");
  }

  @Test
  public void fromSyncIngestBatchSkipsGraphqlRequestContextWithoutUiSource() {
    OperationContext opContext = graphqlOperationContext();
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:child");
    BatchItem item = Mockito.mock(BatchItem.class);
    Mockito.when(item.getUrn()).thenReturn(domainUrn);
    Mockito.when(item.getAspectName()).thenReturn("domainProperties");
    Mockito.when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    Mockito.when(item.getSystemMetadata()).thenReturn(null);
    AspectsBatch batch = Mockito.mock(AspectsBatch.class);
    Mockito.doReturn(List.of(item)).when(batch).getItems();

    SyncGraphInvalidationBatch invalidation =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, batch, Collections.emptyList());

    assertTrue(invalidation.isEmpty());
  }

  private static AspectsBatch syncAspectsBatch(Urn urn, String aspectName, ChangeType changeType) {
    BatchItem item = syncBatchItem(urn, aspectName, changeType);
    AspectsBatch batch = Mockito.mock(AspectsBatch.class);
    Mockito.doReturn(List.of(item)).when(batch).getItems();
    return batch;
  }

  private static UpdateAspectResult materialDeleteResult(
      Urn urn, String aspectName, ChangeType changeType) {
    ChangeMCP request = Mockito.mock(ChangeMCP.class);
    Mockito.when(request.getAspectName()).thenReturn(aspectName);
    Mockito.when(request.getChangeType()).thenReturn(changeType);
    return UpdateAspectResult.builder()
        .urn(urn)
        .request(request)
        .newSystemMetadata(new SystemMetadata())
        .build();
  }

  private static BatchItem syncBatchItem(Urn urn, String aspectName, ChangeType changeType) {
    BatchItem item = Mockito.mock(BatchItem.class);
    Mockito.when(item.getUrn()).thenReturn(urn);
    Mockito.when(item.getAspectName()).thenReturn(aspectName);
    Mockito.when(item.getChangeType()).thenReturn(changeType);
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    Mockito.when(item.getSystemMetadata()).thenReturn(systemMetadata);
    return item;
  }

  private static BatchItem syncBatchItem(Urn urn, String aspectName, DomainProperties aspect) {
    return syncBatchItem(urn, aspectName, ChangeType.UPSERT);
  }

  private static OperationContext operationContextWithDomainGraph() {
    OperationContext base = TestOperationContexts.Builder.builder().buildSystemContext();
    RetrieverContext baseRetriever = base.getRetrieverContext();
    EntityGraphCache graphCache = mock(EntityGraphCache.class);
    when(graphCache.getGraphIdsForEntityType("domain")).thenReturn(Set.of("domain"));
    when(graphCache.getCandidateGraphIds("domain", "domainProperties"))
        .thenReturn(Set.of("domain"));
    when(graphCache.getCandidateGraphIds("domain", "institutionalMemory"))
        .thenReturn(Collections.emptySet());
    when(graphCache.getGraphIdsForEntityType("dataset")).thenReturn(Collections.emptySet());
    return TestOperationContexts.Builder.builder()
        .retrieverContextSupplier(
            () ->
                RetrieverContext.builder()
                    .graphRetriever(baseRetriever.getGraphRetriever())
                    .aspectRetriever(baseRetriever.getAspectRetriever())
                    .cachingAspectRetriever(baseRetriever.getCachingAspectRetriever())
                    .searchRetriever(baseRetriever.getSearchRetriever())
                    .entityGraphCache(graphCache)
                    .build())
        .buildSystemContext();
  }

  private static OperationContext operationContextWithMembershipGraph() {
    OperationContext base = TestOperationContexts.Builder.builder().buildSystemContext();
    RetrieverContext baseRetriever = base.getRetrieverContext();
    EntityGraphCache graphCache = mock(EntityGraphCache.class);
    when(graphCache.getCandidateGraphIds("corpuser", "nativeGroupMembership"))
        .thenReturn(Set.of("membership"));
    when(graphCache.getCandidateGraphIds("corpGroup", "nativeGroupMembership"))
        .thenReturn(Set.of("membership"));
    return TestOperationContexts.Builder.builder()
        .retrieverContextSupplier(
            () ->
                RetrieverContext.builder()
                    .graphRetriever(baseRetriever.getGraphRetriever())
                    .aspectRetriever(baseRetriever.getAspectRetriever())
                    .cachingAspectRetriever(baseRetriever.getCachingAspectRetriever())
                    .searchRetriever(baseRetriever.getSearchRetriever())
                    .entityGraphCache(graphCache)
                    .build())
        .buildSystemContext();
  }

  private static OperationContext graphqlOperationContext() {
    OperationContext systemContext = TestOperationContexts.Builder.builder().buildSystemContext();
    return OperationContext.asSession(
        systemContext,
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("")
            .requestAPI(RequestContext.RequestAPI.GRAPHQL)
            .requestID("addGroupMembers")
            .userAgent(""),
        mock(com.datahub.plugins.auth.authorization.Authorizer.class),
        TestOperationContexts.TEST_USER_AUTH,
        true);
  }
}
