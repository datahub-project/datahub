package com.linkedin.metadata.dataproducts.sideeffects;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCPItem;
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
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.RetrieverContext;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(CachingAspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    RelatedEntities relatedEntities =
        new RelatedEntities(
            "DataProductContains",
            TEST_PRODUCT_URN.toString(),
            DATASET_URN_1.toString(),
            RelationshipDirection.INCOMING,
            null);

    List<RelatedEntities> relatedEntitiesList = new ArrayList<>();
    relatedEntitiesList.add(relatedEntities);
    RelatedEntitiesScrollResult relatedEntitiesScrollResult =
        new RelatedEntitiesScrollResult(1, 10, null, relatedEntitiesList);
    when(graphRetriever.scrollRelatedEntities(
            eq(null),
            eq(QueryUtils.newFilter("urn", DATASET_URN_1.toString())),
            eq(null),
            eq(EMPTY_FILTER),
            eq(ImmutableList.of("DataProductContains")),
            eq(QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Collections.emptyList()),
            eq(null),
            eq(10), // Should only ever be one, if ever greater than ten will decrease over time to
            // become consistent
            eq(null),
            eq(null)))
        .thenReturn(relatedEntitiesScrollResult);

    RelatedEntities relatedEntities2 =
        new RelatedEntities(
            "DataProductContains",
            TEST_PRODUCT_URN_2.toString(),
            DATASET_URN_2.toString(),
            RelationshipDirection.INCOMING,
            null);

    List<RelatedEntities> relatedEntitiesList2 = new ArrayList<>();
    relatedEntitiesList2.add(relatedEntities2);
    RelatedEntitiesScrollResult relatedEntitiesScrollResult2 =
        new RelatedEntitiesScrollResult(1, 10, null, relatedEntitiesList2);
    when(graphRetriever.scrollRelatedEntities(
            eq(null),
            eq(QueryUtils.newFilter("urn", DATASET_URN_2.toString())),
            eq(null),
            eq(EMPTY_FILTER),
            eq(ImmutableList.of("DataProductContains")),
            eq(QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Collections.emptyList()),
            eq(null),
            eq(10), // Should only ever be one, if ever greater than ten will decrease over time to
            // become consistent
            eq(null),
            eq(null)))
        .thenReturn(relatedEntitiesScrollResult2);
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(graphRetriever)
            .build();
  }

  @Test
  public void testDPAlreadySetToSame() {
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

    // Create 100 dataset URNs and set up their existing relationships
    List<Urn> datasetUrns = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Urn datasetUrn =
          UrnUtils.getUrn(
              String.format("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_%d,PROD)", i));
      datasetUrns.add(datasetUrn);

      // Mock the existing relationship for each dataset with the old data product
      RelatedEntities relatedEntities =
          new RelatedEntities(
              "DataProductContains",
              TEST_PRODUCT_URN_2.toString(), // Old data product
              datasetUrn.toString(),
              RelationshipDirection.INCOMING,
              null);

      List<RelatedEntities> relatedEntitiesList = new ArrayList<>();
      relatedEntitiesList.add(relatedEntities);
      RelatedEntitiesScrollResult relatedEntitiesScrollResult =
          new RelatedEntitiesScrollResult(1, 10, null, relatedEntitiesList);

      when(retrieverContext
              .getGraphRetriever()
              .scrollRelatedEntities(
                  eq(null),
                  eq(QueryUtils.newFilter("urn", datasetUrn.toString())),
                  eq(null),
                  eq(EMPTY_FILTER),
                  eq(ImmutableList.of("DataProductContains")),
                  eq(
                      QueryUtils.newRelationshipFilter(
                          EMPTY_FILTER, RelationshipDirection.INCOMING)),
                  eq(Collections.emptyList()),
                  eq(null),
                  eq(10),
                  eq(null),
                  eq(null)))
          .thenReturn(relatedEntitiesScrollResult);
    }

    // Create data product properties with all 100 assets
    DataProductProperties dataProductProperties = new DataProductProperties();
    DataProductAssociationArray dataProductAssociations = new DataProductAssociationArray();
    for (Urn datasetUrn : datasetUrns) {
      DataProductAssociation association = new DataProductAssociation();
      association.setDestinationUrn(datasetUrn);
      dataProductAssociations.add(association);
    }
    dataProductProperties.setAssets(dataProductAssociations);

    // Run test
    ChangeItemImpl dataProductPropertiesChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_PRODUCT_URN) // New data product
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
    assertEquals(testOutput.size(), 1, "Expected one patch to remove assets from old data product");

    MCPItem patchItem = testOutput.get(0);
    assertEquals(
        patchItem.getUrn(), TEST_PRODUCT_URN_2, "Patch should target the old data product");
    assertEquals(patchItem.getAspectName(), DATA_PRODUCT_PROPERTIES_ASPECT_NAME);

    // Verify the patch contains remove operations for all 100 assets
    JsonArray patchArray = ((PatchItemImpl) patchItem).getPatch().toJsonArray();
    assertEquals(patchArray.size(), 100, "Should have 100 remove operations");

    // Verify each remove operation
    for (int i = 0; i < 100; i++) {
      JsonObject op = patchArray.getJsonObject(i);
      assertEquals(op.getString("op"), PatchOperationType.REMOVE.getValue());
      assertEquals(
          op.getString("path"),
          String.format("/assets/urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_%d,PROD)", i));
    }
  }

  @Test
  public void testUpsertWithPreviousAspect() {
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
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            dataProductPropertiesChangeItem,
                            null,
                            null,
                            retrieverContext.getAspectRetriever())),
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
    when(prevData.getRecordTemplate()).thenReturn(sameProperties);

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
                List.of(
                    MCLItemImpl.builder()
                        .build(noChangeItem, null, null, retrieverContext.getAspectRetriever())),
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
