package com.linkedin.metadata.dataproducts.sideeffects;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCTS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProductAssetsSideEffectTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final String DATA_PRODUCTS_FIELD_NAME = "dataProducts";

  private static final Urn PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:ads");
  private static final Urn DATASET_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)");
  private static final Urn DATASET_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)");

  private static final AspectPluginConfig CONFIG =
      AspectPluginConfig.builder()
          .className(DataProductAssetsSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "RESTATE"))
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
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
  }

  private static DataProductProperties propsWith(Urn... assets) {
    DataProductProperties props = new DataProductProperties();
    DataProductAssociationArray associations = new DataProductAssociationArray();
    for (Urn asset : assets) {
      DataProductAssociation association = new DataProductAssociation();
      association.setDestinationUrn(asset);
      associations.add(association);
    }
    props.setAssets(associations);
    return props;
  }

  private ChangeItemImpl changeItem(DataProductProperties current, ChangeType changeType) {
    return ChangeItemImpl.builder()
        .urn(PRODUCT_URN)
        .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
        .changeType(changeType)
        .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
        .aspectSpec(
            TEST_REGISTRY
                .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
        .recordTemplate(current)
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build(mockAspectRetriever);
  }

  private List<MCPItem> run(ChangeItemImpl change, DataProductProperties previous) {
    DataProductAssetsSideEffect test = new DataProductAssetsSideEffect();
    test.setConfig(CONFIG);
    return test.postMCPSideEffect(
            OperationFingerprint.EMPTY,
            List.of(
                MCLItemImpl.builder()
                    .build(change, previous, null, retrieverContext.getAspectRetriever())),
            retrieverContext)
        .toList();
  }

  private PatchItemImpl expectedAssetPatch(
      Urn assetUrn, PatchOperationType operation, ChangeItemImpl source) {
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(operation.getValue());
    patchOp.setPath(String.format("/%s/%s", DATA_PRODUCTS_FIELD_NAME, PRODUCT_URN));
    if (operation == PatchOperationType.ADD) {
      patchOp.setValue(PRODUCT_URN.toString());
    }
    return PatchItemImpl.builder()
        .urn(assetUrn)
        .aspectName(DATA_PRODUCTS_ASPECT_NAME)
        .patch(
            GenericJsonPatch.builder()
                .arrayPrimaryKeys(Map.of(DATA_PRODUCTS_FIELD_NAME, List.<String>of()))
                .patch(List.of(patchOp))
                .build()
                .getJsonPatch())
        .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
        .aspectSpec(
            TEST_REGISTRY
                .getEntitySpec(DATASET_ENTITY_NAME)
                .getAspectSpec(DATA_PRODUCTS_ASPECT_NAME))
        .auditStamp(source.getAuditStamp())
        .systemMetadata(source.getSystemMetadata())
        .build(mockAspectRetriever.getEntityRegistry());
  }

  @Test
  public void testAddedAssetGetsAddPatch() {
    ChangeItemImpl change = changeItem(propsWith(DATASET_1, DATASET_2), ChangeType.UPSERT);
    List<MCPItem> output = run(change, propsWith(DATASET_1));

    assertEquals(output.size(), 1, "Expected one ADD patch for the newly-added asset: " + output);
    assertEquals(
        output.get(0).getUrn(),
        DATASET_2,
        "ADD patch should target the added asset's dataProducts");
    assertEquals(output.get(0).getAspectName(), DATA_PRODUCTS_ASPECT_NAME);
    assertEquals(output.get(0), expectedAssetPatch(DATASET_2, PatchOperationType.ADD, change));
  }

  @Test
  public void testRemovedAssetGetsRemovePatch() {
    ChangeItemImpl change = changeItem(propsWith(DATASET_1), ChangeType.UPSERT);
    List<MCPItem> output = run(change, propsWith(DATASET_1, DATASET_2));

    assertEquals(output.size(), 1, "Expected one REMOVE patch for the dropped asset: " + output);
    assertEquals(output.get(0).getUrn(), DATASET_2);
    assertEquals(output.get(0), expectedAssetPatch(DATASET_2, PatchOperationType.REMOVE, change));
  }

  @Test
  public void testCreateTreatsAllAssetsAsAdded() {
    ChangeItemImpl change = changeItem(propsWith(DATASET_1), ChangeType.CREATE);
    List<MCPItem> output = run(change, null);

    assertEquals(output.size(), 1, "Expected all assets treated as added on create: " + output);
    assertEquals(output.get(0).getUrn(), DATASET_1);
    assertEquals(output.get(0), expectedAssetPatch(DATASET_1, PatchOperationType.ADD, change));
  }

  @Test
  public void testNoChangeYieldsNoPatches() {
    ChangeItemImpl change = changeItem(propsWith(DATASET_1, DATASET_2), ChangeType.UPSERT);
    List<MCPItem> output = run(change, propsWith(DATASET_1, DATASET_2));

    assertEquals(output.size(), 0, "Expected no patches when membership is unchanged: " + output);
  }
}
