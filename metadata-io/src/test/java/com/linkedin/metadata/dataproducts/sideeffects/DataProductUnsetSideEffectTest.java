package com.linkedin.metadata.dataproducts.sideeffects;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.template.dataproduct.DataProductPropertiesTemplate;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.RetrieverContext;
import jakarta.json.JsonArray;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProductUnsetSideEffectTest {
  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final List<ChangeType> SUPPORTED_CHANGE_TYPES =
      List.of(ChangeType.CREATE, ChangeType.CREATE_ENTITY, ChangeType.UPSERT, ChangeType.RESTATE);
  private static final Urn TEST_PRODUCT_URN =
      UrnUtils.getUrn("urn:li:dataProduct:someDataProductId");

  private static final Urn TEST_PRODUCT_URN_2 =
      UrnUtils.getUrn("urn:li:dataProduct:someOtherDataProductId");

  private static final Urn TEST_PRODUCT_URN_3 =
      UrnUtils.getUrn("urn:li:dataProduct:thirdDataProductId");

  private static final Urn DATASET_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)");
  private static final Urn DATASET_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)");
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(DataProductUnsetSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(
              SUPPORTED_CHANGE_TYPES.stream()
                  .map(ChangeType::toString)
                  .collect(Collectors.toList()))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(DATA_PRODUCT_ENTITY_NAME)
                      .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
                      .build()))
          .build();

  private CachingAspectRetriever mockAspectRetriever;
  private GraphRetriever graphRetriever;
  private RetrieverContext retrieverContext;
  private final Map<String, Urn> existingProductByAsset = new HashMap<>();

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(CachingAspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    graphRetriever = mock(GraphRetriever.class);
    existingProductByAsset.clear();
    stubBatchGraphScroll();
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(graphRetriever)
            .build();
  }

  private void registerAssetInProduct(Urn assetUrn, Urn dataProductUrn) {
    existingProductByAsset.put(assetUrn.toString(), dataProductUrn);
  }

  private void stubBatchGraphScroll() {
    when(graphRetriever.scrollRelatedEntities(
            isNull(),
            any(),
            isNull(),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of("DataProductContains")),
            eq(QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            any(),
            eq(DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE),
            isNull(),
            isNull()))
        .thenAnswer(
            invocation -> {
              Set<String> requestedUrns = extractUrnFilterValues(invocation.getArgument(1));
              List<RelatedEntities> entities = new ArrayList<>();
              for (Map.Entry<String, Urn> entry : existingProductByAsset.entrySet()) {
                if (requestedUrns.isEmpty() || requestedUrns.contains(entry.getKey())) {
                  entities.add(
                      new RelatedEntities(
                          "DataProductContains",
                          entry.getValue().toString(),
                          entry.getKey(),
                          RelationshipDirection.INCOMING,
                          null));
                }
              }
              return new RelatedEntitiesScrollResult(
                  entities.size(),
                  DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE,
                  null,
                  entities);
            });
  }

  private MCLItemImpl buildMclItem(ChangeItemImpl changeItem, RecordTemplate previousAspect) {
    return MCLItemImpl.builder()
        .build(changeItem, previousAspect, null, retrieverContext.getAspectRetriever());
  }

  private static Set<String> extractUrnFilterValues(Filter filter) {
    Set<String> urns = new HashSet<>();
    if (filter == null || filter.getOr() == null) {
      return urns;
    }
    for (ConjunctiveCriterion conjunction : filter.getOr()) {
      if (conjunction.getAnd() == null) {
        continue;
      }
      for (Criterion criterion : conjunction.getAnd()) {
        if ("urn".equals(criterion.getField()) && criterion.getValues() != null) {
          urns.addAll(criterion.getValues());
        }
      }
    }
    return urns;
  }

  @Test
  public void testDPAlreadySetToSame() {
    registerAssetInProduct(DATASET_URN_1, TEST_PRODUCT_URN);
    DataProductUnsetSideEffect test = new DataProductUnsetSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    DataProductProperties dataProductProperties = getTestDataProductProperties(DATASET_URN_1);

    List<MCPItem> testOutput;
    // Run test
    ChangeItemImpl dataProductPropertiesChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_PRODUCT_URN)
            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                    .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
            .recordTemplate(dataProductProperties)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);
    testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            dataProductPropertiesChangeItem,
                            null,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    // Verify test
    assertEquals(testOutput.size(), 0, "Expected no additional changes: " + testOutput);
  }

  @Test
  public void testDPRemoveOld() {
    registerAssetInProduct(DATASET_URN_2, TEST_PRODUCT_URN_2);
    DataProductUnsetSideEffect test = new DataProductUnsetSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    DataProductProperties dataProductProperties = getTestDataProductProperties(DATASET_URN_2);

    List<MCPItem> testOutput;
    // Run test
    ChangeItemImpl dataProductPropertiesChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_PRODUCT_URN)
            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                    .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
            .recordTemplate(dataProductProperties)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);
    testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            dataProductPropertiesChangeItem,
                            null,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    // Verify test
    assertEquals(testOutput.size(), 1, "Expected removal of previous data product: " + testOutput);

    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(PatchOperationType.REMOVE.getValue());
    patchOp.setPath(String.format("/assets/%s", DATASET_URN_2));

    assertEquals(
        testOutput,
        List.of(
            PatchItemImpl.builder()
                .urn(TEST_PRODUCT_URN_2)
                .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
                .patch(
                    GenericJsonPatch.builder()
                        .arrayPrimaryKeys(
                            Map.of(
                                DataProductPropertiesTemplate.ASSETS_FIELD_NAME,
                                List.of(DataProductPropertiesTemplate.KEY_FIELD_NAME)))
                        .patch(List.of(patchOp))
                        .build()
                        .getJsonPatch())
                .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
                .aspectSpec(
                    TEST_REGISTRY
                        .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                        .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
                .auditStamp(dataProductPropertiesChangeItem.getAuditStamp())
                .systemMetadata(dataProductPropertiesChangeItem.getSystemMetadata())
                .build(mockAspectRetriever.getEntityRegistry())));
  }

  @Test
  public void testBulkAssetMove() {
    DataProductUnsetSideEffect test = new DataProductUnsetSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    List<Urn> datasetUrns = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Urn datasetUrn =
          UrnUtils.getUrn(
              String.format("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_%d,PROD)", i));
      datasetUrns.add(datasetUrn);
      registerAssetInProduct(datasetUrn, TEST_PRODUCT_URN_2);
    }

    DataProductProperties dataProductProperties = new DataProductProperties();
    DataProductAssociationArray dataProductAssociations = new DataProductAssociationArray();
    for (Urn datasetUrn : datasetUrns) {
      DataProductAssociation association = new DataProductAssociation();
      association.setDestinationUrn(datasetUrn);
      dataProductAssociations.add(association);
    }
    dataProductProperties.setAssets(dataProductAssociations);

    ChangeItemImpl dataProductPropertiesChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_PRODUCT_URN)
            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                    .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
            .recordTemplate(dataProductProperties)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            dataProductPropertiesChangeItem,
                            null,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(testOutput.size(), 1, "Expected one patch to remove assets from old data product");
    verify(graphRetriever, atMost(1))
        .scrollRelatedEntities(
            isNull(),
            any(),
            isNull(),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of("DataProductContains")),
            eq(QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            any(),
            eq(DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE),
            isNull(),
            isNull());

    MCPItem patchItem = testOutput.get(0);
    assertEquals(
        patchItem.getUrn(), TEST_PRODUCT_URN_2, "Patch should target the old data product");
    assertEquals(patchItem.getAspectName(), DATA_PRODUCT_PROPERTIES_ASPECT_NAME);

    JsonArray patchArray = ((PatchItemImpl) patchItem).getPatch().toJsonArray();
    assertEquals(patchArray.size(), 100, "Should have 100 remove operations");
  }

  @Test
  public void testBulkAssetMoveUsesMultipleChunks() {
    DataProductUnsetSideEffect test = new DataProductUnsetSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    List<Urn> datasetUrns = new ArrayList<>();
    for (int i = 0; i < 150; i++) {
      Urn datasetUrn =
          UrnUtils.getUrn(
              String.format("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_%d,PROD)", i));
      datasetUrns.add(datasetUrn);
      registerAssetInProduct(datasetUrn, TEST_PRODUCT_URN_2);
    }

    DataProductProperties dataProductProperties = new DataProductProperties();
    DataProductAssociationArray dataProductAssociations = new DataProductAssociationArray();
    for (Urn datasetUrn : datasetUrns) {
      DataProductAssociation association = new DataProductAssociation();
      association.setDestinationUrn(datasetUrn);
      dataProductAssociations.add(association);
    }
    dataProductProperties.setAssets(dataProductAssociations);

    ChangeItemImpl dataProductPropertiesChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_PRODUCT_URN)
            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                    .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
            .recordTemplate(dataProductProperties)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            dataProductPropertiesChangeItem,
                            null,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(testOutput.size(), 1, "Expected one patch to remove assets from old data product");
    verify(graphRetriever, times(2))
        .scrollRelatedEntities(
            isNull(),
            any(),
            isNull(),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of("DataProductContains")),
            eq(QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            any(),
            eq(DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE),
            isNull(),
            isNull());

    JsonArray patchArray = ((PatchItemImpl) testOutput.get(0)).getPatch().toJsonArray();
    assertEquals(patchArray.size(), 150, "Should have 150 remove operations");
  }

  @Test
  public void testGraphScrollPaginatesUntilScrollIdExhausted() {
    reset(graphRetriever);
    DataProductUnsetSideEffect test = new DataProductUnsetSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    RelatedEntities firstPage =
        new RelatedEntities(
            "DataProductContains",
            TEST_PRODUCT_URN_2.toString(),
            DATASET_URN_2.toString(),
            RelationshipDirection.INCOMING,
            null);
    RelatedEntities secondPage =
        new RelatedEntities(
            "DataProductContains",
            TEST_PRODUCT_URN_3.toString(),
            DATASET_URN_2.toString(),
            RelationshipDirection.INCOMING,
            null);

    when(graphRetriever.scrollRelatedEntities(
            isNull(),
            any(),
            isNull(),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of("DataProductContains")),
            eq(QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            isNull(),
            eq(DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1,
                DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE,
                "page-2",
                List.of(firstPage)));

    when(graphRetriever.scrollRelatedEntities(
            isNull(),
            any(),
            isNull(),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of("DataProductContains")),
            eq(QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            eq("page-2"),
            eq(DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1, DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE, null, List.of(secondPage)));

    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(TEST_PRODUCT_URN)
            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                    .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
            .recordTemplate(getTestDataProductProperties(DATASET_URN_2))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> output =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(changeItem, null, null, retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(output.size(), 2, "Both paginated graph pages should produce unset patches");
    verify(graphRetriever, times(2))
        .scrollRelatedEntities(
            isNull(),
            any(),
            isNull(),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of("DataProductContains")),
            eq(QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            any(),
            eq(DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE),
            isNull(),
            isNull());
  }

  @Test
  public void testUpsertWithOnlyAuditStampChangeSkipsGraphScroll() {
    DataProductUnsetSideEffect test = new DataProductUnsetSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    DataProductProperties previousProperties = new DataProductProperties();
    DataProductAssociation previousAssociation = new DataProductAssociation();
    previousAssociation.setDestinationUrn(DATASET_URN_1);
    previousAssociation.setCreated(
        new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:old")));
    DataProductAssociationArray previousAssociations = new DataProductAssociationArray();
    previousAssociations.add(previousAssociation);
    previousProperties.setAssets(previousAssociations);

    DataProductProperties newProperties = new DataProductProperties();
    DataProductAssociation newAssociation = new DataProductAssociation();
    newAssociation.setDestinationUrn(DATASET_URN_1);
    newAssociation.setCreated(
        new AuditStamp().setTime(2L).setActor(UrnUtils.getUrn("urn:li:corpuser:new")));
    DataProductAssociationArray newAssociations = new DataProductAssociationArray();
    newAssociations.add(newAssociation);
    newProperties.setAssets(newAssociations);

    SystemAspect prevData = mock(SystemAspect.class);
    when(prevData.getRecordTemplate()).thenReturn(previousProperties);

    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(TEST_PRODUCT_URN)
            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                    .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
            .recordTemplate(newProperties)
            .previousSystemAspect(prevData)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> output =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(buildMclItem(changeItem, previousProperties)),
                retrieverContext)
            .toList();

    assertTrue(output.isEmpty(), "Unchanged destination URNs should not trigger graph scroll");
    verify(graphRetriever, never())
        .scrollRelatedEntities(
            isNull(),
            any(),
            isNull(),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of("DataProductContains")),
            eq(QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            any(),
            eq(DataProductUnsetSideEffect.GRAPH_SCROLL_CHUNK_SIZE),
            isNull(),
            isNull());
  }

  @Test
  public void testRestateWithPreviousUsesDeltaOnly() {
    registerAssetInProduct(DATASET_URN_1, TEST_PRODUCT_URN_3);
    registerAssetInProduct(DATASET_URN_2, TEST_PRODUCT_URN_2);
    DataProductUnsetSideEffect test = new DataProductUnsetSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    DataProductProperties previousProperties = getTestDataProductProperties(DATASET_URN_1);
    DataProductProperties newProperties = new DataProductProperties();
    DataProductAssociationArray newAssociations = new DataProductAssociationArray();
    DataProductAssociation existing = new DataProductAssociation();
    existing.setDestinationUrn(DATASET_URN_1);
    DataProductAssociation added = new DataProductAssociation();
    added.setDestinationUrn(DATASET_URN_2);
    newAssociations.add(existing);
    newAssociations.add(added);
    newProperties.setAssets(newAssociations);

    MCLItemImpl mclItem =
        MCLItemImpl.builder()
            .metadataChangeLog(
                new MetadataChangeLog()
                    .setEntityUrn(TEST_PRODUCT_URN)
                    .setEntityType(DATA_PRODUCT_ENTITY_NAME)
                    .setChangeType(ChangeType.RESTATE)
                    .setAspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
                    .setAspect(GenericRecordUtils.serializeAspect(newProperties))
                    .setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousProperties))
                    .setCreated(AuditStampUtils.createDefaultAuditStamp()))
            .build(retrieverContext.getAspectRetriever());

    List<MCPItem> output =
        test.postMCPSideEffect(OperationFingerprint.EMPTY, List.of(mclItem), retrieverContext)
            .toList();

    assertEquals(output.size(), 1, "Only the newly added asset should trigger unset");
    assertEquals(output.get(0).getUrn(), TEST_PRODUCT_URN_2);
  }

  @Test
  public void testUpsertWithPreviousAspect() {
    registerAssetInProduct(DATASET_URN_2, TEST_PRODUCT_URN_2);
    DataProductUnsetSideEffect test = new DataProductUnsetSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    // Case 1: UPSERT with new additions
    DataProductProperties previousProperties = new DataProductProperties();
    DataProductAssociationArray previousAssociations = new DataProductAssociationArray();
    DataProductAssociation previousAssociation = new DataProductAssociation();
    previousAssociation.setDestinationUrn(DATASET_URN_1);
    previousAssociations.add(previousAssociation);
    previousProperties.setAssets(previousAssociations);

    // New properties include both old and new datasets
    DataProductProperties newProperties = new DataProductProperties();
    DataProductAssociationArray newAssociations = new DataProductAssociationArray();
    DataProductAssociation association1 = new DataProductAssociation();
    association1.setDestinationUrn(DATASET_URN_1);
    DataProductAssociation association2 = new DataProductAssociation();
    association2.setDestinationUrn(DATASET_URN_2);
    newAssociations.add(association1);
    newAssociations.add(association2);
    newProperties.setAssets(newAssociations);

    // Create change item with previous aspect
    SystemAspect prevData = mock(SystemAspect.class);
    when(prevData.getRecordTemplate()).thenReturn(previousProperties);

    ChangeItemImpl dataProductPropertiesChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_PRODUCT_URN)
            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                    .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
            .recordTemplate(newProperties)
            .previousSystemAspect(prevData)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(buildMclItem(dataProductPropertiesChangeItem, previousProperties)),
                retrieverContext)
            .toList();

    // Verify that only one patch is generated for the new dataset
    assertEquals(
        testOutput.size(), 1, "Expected removal of previous data product for new dataset only");
    MCPItem patchItem = testOutput.get(0);
    assertEquals(
        patchItem.getUrn(), TEST_PRODUCT_URN_2, "Patch should target the old data product");
    GenericJsonPatch.PatchOp expectedPatchOp = new GenericJsonPatch.PatchOp();
    expectedPatchOp.setOp(PatchOperationType.REMOVE.getValue());
    expectedPatchOp.setPath(String.format("/assets/%s", DATASET_URN_2));

    // Case 2: UPSERT with no new additions
    DataProductProperties sameProperties = new DataProductProperties();
    DataProductAssociationArray sameAssociations = new DataProductAssociationArray();
    DataProductAssociation sameAssociation = new DataProductAssociation();
    sameAssociation.setDestinationUrn(DATASET_URN_1);
    sameAssociations.add(sameAssociation);
    sameProperties.setAssets(sameAssociations);

    SystemAspect prevSameData = mock(SystemAspect.class);
    when(prevSameData.getRecordTemplate()).thenReturn(sameProperties);

    ChangeItemImpl noChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_PRODUCT_URN)
            .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                    .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
            .recordTemplate(sameProperties)
            .previousSystemAspect(prevSameData)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> noChangeOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(buildMclItem(noChangeItem, sameProperties)),
                retrieverContext)
            .toList();

    // Verify no patches are generated when there are no new additions
    assertEquals(noChangeOutput.size(), 0, "Expected no changes when assets are the same");
  }

  private static DataProductProperties getTestDataProductProperties(Urn destinationUrn) {
    DataProductProperties dataProductProperties = new DataProductProperties();
    DataProductAssociationArray dataProductAssociations = new DataProductAssociationArray();
    DataProductAssociation dataProductAssociation1 = new DataProductAssociation();
    dataProductAssociation1.setDestinationUrn(destinationUrn);
    dataProductAssociations.add(dataProductAssociation1);
    dataProductProperties.setAssets(dataProductAssociations);
    return dataProductProperties;
  }
}
