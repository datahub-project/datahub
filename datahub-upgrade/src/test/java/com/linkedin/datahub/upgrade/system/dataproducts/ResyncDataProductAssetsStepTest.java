package com.linkedin.datahub.upgrade.system.dataproducts;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATA_PRODUCTS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.dataproducts.sideeffects.DataProductAssetsSideEffect;
import com.linkedin.metadata.dataproducts.sideeffects.DataProductUnsetSideEffect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ResyncDataProductAssetsStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final Urn PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:ads");
  private static final Urn DATASET_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)");
  private static final Urn DATASET_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)");
  private static final AuditStamp AUDIT_STAMP =
      new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(0L);

  private EntityService<?> mockEntityService;
  private AspectDao mockAspectDao;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockAspectDao = mock(AspectDao.class);
  }

  @Test
  public void testIdAndUrnLike() {
    ResyncDataProductAssetsStep step = newStep(false);
    assertEquals(step.id(), "data-product-assets-from-properties-v1");
    assertEquals(step.getUrnLike(), "urn:li:dataProduct:%");
  }

  @Test
  public void testSkipWhenReprocessEnabled() {
    ResyncDataProductAssetsStep step = newStep(true);
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult succeeded = mock(DataHubUpgradeResult.class);
    when(succeeded.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(succeeded));

    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testSkipWhenAlreadySucceeded() {
    ResyncDataProductAssetsStep step = newStep(false);
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult succeeded = mock(DataHubUpgradeResult.class);
    when(succeeded.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(), eq(mockEntityService)))
        .thenReturn(Optional.of(succeeded));

    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testDoesNotSkipWhenNoPreviousResult() {
    ResyncDataProductAssetsStep step = newStep(false);
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testPartitionChunksBySize() {
    List<Integer> items = List.of(1, 2, 3, 4, 5, 6, 7);
    List<List<Integer>> chunks = ResyncDataProductAssetsStep.partition(items, 3);
    assertEquals(chunks.size(), 3);
    assertEquals(chunks.get(0), List.of(1, 2, 3));
    assertEquals(chunks.get(1), List.of(4, 5, 6));
    assertEquals(chunks.get(2), List.of(7));
    assertTrue(ResyncDataProductAssetsStep.partition(List.of(), 3).isEmpty());
  }

  @Test
  public void testToRestateMclItemsStampsRestateAndSystemUpdateSource() {
    ResyncDataProductAssetsStep step = newStep(false);
    SystemAspect systemAspect = mockPropertiesSystemAspect(OP_CONTEXT, DATASET_1, DATASET_2);

    List<MCLItem> restateItems = step.toRestateMclItems(List.of(systemAspect));
    assertEquals(restateItems.size(), 1);
    assertEquals(restateItems.get(0).getChangeType(), ChangeType.RESTATE);
    assertEquals(restateItems.get(0).getUrn(), PRODUCT_URN);
    assertEquals(restateItems.get(0).getAspectName(), DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    assertEquals(
        restateItems.get(0).getSystemMetadata().getProperties().get(APP_SOURCE),
        SYSTEM_UPDATE_SOURCE);
    assertNotNull(restateItems.get(0).getPreviousAspect(DataProductProperties.class));
  }

  @Test
  public void testBuildSideEffectProposalsEmitsAssetPatches() {
    EntityRegistry entityRegistry = OP_CONTEXT.getEntityRegistry();
    DataProductAssetsSideEffect sideEffect = dataProductAssetsSideEffect();
    EntityRegistry spyRegistry = org.mockito.Mockito.spy(entityRegistry);
    when(spyRegistry.getAllMCPSideEffects()).thenReturn(List.of(sideEffect));

    com.linkedin.metadata.aspect.AspectRetriever mockAspectRetriever =
        mock(com.linkedin.metadata.aspect.AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(spyRegistry);
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(java.util.Map.of());

    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);
    ResyncDataProductAssetsStep step =
        new ResyncDataProductAssetsStep(
            opContext, mockEntityService, mockAspectDao, 10, 0, 1000, false);

    List<MCLItem> mclItems =
        step.toRestateMclItems(
            List.of(mockPropertiesSystemAspect(opContext, DATASET_1, DATASET_2)));
    List<MCPItem> proposals = step.buildSideEffectProposals(mclItems);

    List<MCPItem> assetPatches =
        proposals.stream()
            .filter(item -> DATA_PRODUCTS_ASPECT_NAME.equals(item.getAspectName()))
            .toList();
    assertEquals(assetPatches.size(), 2);
    assertTrue(assetPatches.stream().anyMatch(item -> DATASET_1.equals(item.getUrn())));
    assertTrue(assetPatches.stream().anyMatch(item -> DATASET_2.equals(item.getUrn())));
  }

  @Test
  public void testBuildSideEffectProposalsWithUnsetRegisteredEmitsNoPropertiesPatches() {
    EntityRegistry entityRegistry = OP_CONTEXT.getEntityRegistry();
    EntityRegistry spyRegistry = org.mockito.Mockito.spy(entityRegistry);
    when(spyRegistry.getAllMCPSideEffects())
        .thenReturn(List.of(dataProductAssetsSideEffect(), dataProductUnsetSideEffect()));

    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(spyRegistry);
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(java.util.Map.of());

    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(mockAspectRetriever)
            .cachingAspectRetriever(
                TestOperationContexts.emptyActiveUsersAspectRetriever(() -> spyRegistry))
            .graphRetriever(mockGraphRetriever)
            .searchRetriever(SearchRetriever.EMPTY)
            .build();

    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(retrieverContext);
    ResyncDataProductAssetsStep step =
        new ResyncDataProductAssetsStep(
            opContext, mockEntityService, mockAspectDao, 10, 0, 1000, false);

    List<MCLItem> mclItems =
        step.toRestateMclItems(
            List.of(mockPropertiesSystemAspect(opContext, DATASET_1, DATASET_2)));
    List<MCPItem> proposals = step.buildSideEffectProposals(mclItems);

    List<MCPItem> assetPatches =
        proposals.stream()
            .filter(item -> DATA_PRODUCTS_ASPECT_NAME.equals(item.getAspectName()))
            .toList();
    assertEquals(assetPatches.size(), 2);
    assertTrue(assetPatches.stream().anyMatch(item -> DATASET_1.equals(item.getUrn())));
    assertTrue(assetPatches.stream().anyMatch(item -> DATASET_2.equals(item.getUrn())));
    assertTrue(
        proposals.stream()
            .noneMatch(item -> DATA_PRODUCT_PROPERTIES_ASPECT_NAME.equals(item.getAspectName())));
    verify(mockGraphRetriever, never())
        .scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testIngestSideEffectProposalsChunksByBatchSize() {
    ResyncDataProductAssetsStep step =
        new ResyncDataProductAssetsStep(
            OP_CONTEXT, mockEntityService, mockAspectDao, 5, 0, 1000, false);

    List<MCPItem> proposals = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      proposals.add(mock(MCPItem.class));
    }

    step.ingestSideEffectProposals(proposals);

    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(3))
        .ingestProposal(eq(OP_CONTEXT), batchCaptor.capture(), eq(true));

    List<AspectsBatch> batches = batchCaptor.getAllValues();
    assertEquals(batches.get(0).getItems().size(), 5);
    assertEquals(batches.get(1).getItems().size(), 5);
    assertEquals(batches.get(2).getItems().size(), 2);
  }

  @Test
  public void testIngestSideEffectProposalsNoOpWhenEmpty() {
    ResyncDataProductAssetsStep step = newStep(false);
    step.ingestSideEffectProposals(List.of());
    verify(mockEntityService, times(0))
        .ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
  }

  @Test
  public void testExecutableEmptyScanSucceedsAndRecordsMarker() {
    ResyncDataProductAssetsStep step = newStep(false);
    UpgradeContext mockContext = mockUpgradeContext();

    PartitionedStream<com.linkedin.metadata.entity.ebean.EbeanAspectV2> partitionedStream =
        mock(PartitionedStream.class);
    when(partitionedStream.partition(org.mockito.ArgumentMatchers.anyInt()))
        .thenReturn(Stream.empty());
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class), any()))
        .thenAnswer(
            invocation ->
                ((java.util.function.Function<
                            PartitionedStream<com.linkedin.metadata.entity.ebean.EbeanAspectV2>,
                            Object>)
                        invocation.getArgument(2))
                    .apply(partitionedStream));

    UpgradeStepResult result = step.executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao).streamAspectBatches(eq(OP_CONTEXT), argsCaptor.capture(), any());
    assertEquals(argsCaptor.getValue().urnLike, "urn:li:dataProduct:%");
    assertEquals(argsCaptor.getValue().aspectNames, List.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME));
  }

  private ResyncDataProductAssetsStep newStep(boolean reprocess) {
    return new ResyncDataProductAssetsStep(
        OP_CONTEXT, mockEntityService, mockAspectDao, 10, 0, 1000, reprocess);
  }

  private UpgradeContext mockUpgradeContext() {
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.opContext()).thenReturn(OP_CONTEXT);
    when(mockContext.report()).thenReturn(mock(UpgradeReport.class));
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
    return mockContext;
  }

  private static DataProductAssetsSideEffect dataProductAssetsSideEffect() {
    return new DataProductAssetsSideEffect()
        .setConfig(
            AspectPluginConfig.builder()
                .enabled(true)
                .className(DataProductAssetsSideEffect.class.getName())
                .supportedOperations(
                    List.of("CREATE", "CREATE_ENTITY", "UPSERT", "RESTATE", "DELETE"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(DATA_PRODUCT_ENTITY_NAME)
                            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
                            .build()))
                .build());
  }

  private static DataProductUnsetSideEffect dataProductUnsetSideEffect() {
    return new DataProductUnsetSideEffect()
        .setConfig(
            AspectPluginConfig.builder()
                .enabled(true)
                .className(DataProductUnsetSideEffect.class.getName())
                .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "RESTATE"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(DATA_PRODUCT_ENTITY_NAME)
                            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
                            .build()))
                .build());
  }

  private static SystemAspect mockPropertiesSystemAspect(
      OperationContext opContext, Urn... assets) {
    EntitySpec spec = opContext.getEntityRegistry().getEntitySpec(DATA_PRODUCT_ENTITY_NAME);
    DataProductProperties properties = new DataProductProperties();
    DataProductAssociationArray associations = new DataProductAssociationArray();
    for (Urn asset : assets) {
      DataProductAssociation association = new DataProductAssociation();
      association.setDestinationUrn(asset);
      associations.add(association);
    }
    properties.setAssets(associations);

    SystemAspect systemAspect = mock(SystemAspect.class);
    when(systemAspect.getUrn()).thenReturn(PRODUCT_URN);
    when(systemAspect.getEntitySpec()).thenReturn(spec);
    when(systemAspect.getAspectName()).thenReturn(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    when(systemAspect.getAspectSpec())
        .thenReturn(spec.getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME));
    when(systemAspect.getRecordTemplate()).thenReturn(properties);
    when(systemAspect.getAuditStamp()).thenReturn(AUDIT_STAMP);
    when(systemAspect.getSystemMetadata()).thenReturn(new SystemMetadata());
    return systemAspect;
  }
}
