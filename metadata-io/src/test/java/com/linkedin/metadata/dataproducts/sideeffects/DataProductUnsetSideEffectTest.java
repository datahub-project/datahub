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
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
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
      List.of(
          ChangeType.CREATE,
          ChangeType.PATCH,
          ChangeType.CREATE_ENTITY,
          ChangeType.UPSERT,
          ChangeType.DELETE,
          ChangeType.RESTATE);
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

  private AspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(AspectRetriever.class);
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
            .aspectRetriever(mockAspectRetriever)
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
