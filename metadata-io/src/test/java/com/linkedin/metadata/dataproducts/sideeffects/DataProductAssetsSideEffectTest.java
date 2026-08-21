package com.linkedin.metadata.dataproducts.sideeffects;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCTS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.template.dataproduct.DataProductsTemplate;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProductAssetsSideEffectTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  private static final Urn PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:ads");
  private static final Urn DATASET_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)");
  private static final Urn DATASET_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)");

  private static final AspectPluginConfig CONFIG =
      AspectPluginConfig.builder()
          .className(DataProductAssetsSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "RESTATE", "DELETE"))
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

  private static DataProductProperties propsWith(boolean outputPort, Urn... assets) {
    DataProductProperties props = new DataProductProperties();
    DataProductAssociationArray associations = new DataProductAssociationArray();
    for (Urn asset : assets) {
      DataProductAssociation association = new DataProductAssociation();
      association.setDestinationUrn(asset);
      association.setOutputPort(outputPort);
      associations.add(association);
    }
    props.setAssets(associations);
    return props;
  }

  private static DataProductProperties propsWith(Urn... assets) {
    return propsWith(false, assets);
  }

  private ChangeItemImpl changeItem(
      DataProductProperties current, ChangeType changeType, SystemMetadata systemMetadata) {
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
        .systemMetadata(systemMetadata)
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build(mockAspectRetriever);
  }

  private ChangeItemImpl changeItem(DataProductProperties current, ChangeType changeType) {
    return changeItem(current, changeType, null);
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
      Urn assetUrn, PatchOperationType operation, boolean outputPort, ChangeItemImpl source) {
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(operation.getValue());
    patchOp.setPath(
        String.format("/%s/%s", DataProductsTemplate.DATA_PRODUCTS_FIELD_NAME, PRODUCT_URN));
    if (operation == PatchOperationType.ADD) {
      Map<String, Object> value = new HashMap<>();
      value.put(DataProductsTemplate.KEY_FIELD_NAME, PRODUCT_URN.toString());
      value.put("outputPort", outputPort);
      patchOp.setValue(value);
    }
    return PatchItemImpl.builder()
        .urn(assetUrn)
        .aspectName(DATA_PRODUCTS_ASPECT_NAME)
        .patch(GenericJsonPatch.builder().patch(List.of(patchOp)).build().getJsonPatch())
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
    assertEquals(output.get(0).getUrn(), DATASET_2);
    assertEquals(output.get(0).getAspectName(), DATA_PRODUCTS_ASPECT_NAME);
    assertEquals(
        output.get(0), expectedAssetPatch(DATASET_2, PatchOperationType.ADD, false, change));
  }

  @Test
  public void testRemovedAssetGetsRemovePatch() {
    ChangeItemImpl change = changeItem(propsWith(DATASET_1), ChangeType.UPSERT);
    List<MCPItem> output = run(change, propsWith(DATASET_1, DATASET_2));

    assertEquals(output.size(), 1, "Expected one REMOVE patch for the dropped asset: " + output);
    assertEquals(output.get(0).getUrn(), DATASET_2);
    assertEquals(
        output.get(0), expectedAssetPatch(DATASET_2, PatchOperationType.REMOVE, false, change));
  }

  @Test
  public void testCreateTreatsAllAssetsAsAdded() {
    ChangeItemImpl change = changeItem(propsWith(true, DATASET_1), ChangeType.CREATE);
    List<MCPItem> output = run(change, null);

    assertEquals(output.size(), 1, "Expected all assets treated as added on create: " + output);
    assertEquals(output.get(0).getUrn(), DATASET_1);
    assertEquals(
        output.get(0), expectedAssetPatch(DATASET_1, PatchOperationType.ADD, true, change));
  }

  @Test
  public void testNoChangeYieldsNoPatches() {
    ChangeItemImpl change = changeItem(propsWith(DATASET_1, DATASET_2), ChangeType.UPSERT);
    List<MCPItem> output = run(change, propsWith(DATASET_1, DATASET_2));

    assertEquals(output.size(), 0, "Expected no patches when membership is unchanged: " + output);
  }

  @Test
  public void testSystemUpdateForcesFullAdd() {
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);
    systemMetadata.setProperties(properties);

    ChangeItemImpl change =
        changeItem(propsWith(DATASET_1, DATASET_2), ChangeType.UPSERT, systemMetadata);
    List<MCPItem> output = run(change, propsWith(DATASET_1, DATASET_2));

    assertEquals(
        output.size(),
        2,
        "System-update UPSERT should re-ADD all current members even when unchanged: " + output);
  }

  @Test
  public void testDeleteScrubsPreviousAssets() {
    DataProductAssetsSideEffect test = new DataProductAssetsSideEffect();
    test.setConfig(CONFIG);

    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(PRODUCT_URN);
    mcl.setEntityType(DATA_PRODUCT_ENTITY_NAME);
    mcl.setAspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    mcl.setChangeType(ChangeType.DELETE);
    mcl.setSystemMetadata(new SystemMetadata());

    List<MCPItem> output =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    TestMCL.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(PRODUCT_URN)
                        .entitySpec(TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
                        .aspectSpec(
                            TEST_REGISTRY
                                .getEntitySpec(DATA_PRODUCT_ENTITY_NAME)
                                .getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
                        .previousRecordTemplate(propsWith(DATASET_1, DATASET_2))
                        .metadataChangeLog(mcl)
                        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                        .build()),
                retrieverContext)
            .toList();

    assertEquals(output.size(), 2, "DELETE should REMOVE membership from previous assets");
    assertTrue(output.stream().anyMatch(item -> DATASET_1.equals(item.getUrn())));
    assertTrue(output.stream().anyMatch(item -> DATASET_2.equals(item.getUrn())));
  }

  @Test
  public void testFanoutBoundTruncates() {
    DataProductAssociationArray associations = new DataProductAssociationArray();
    for (int i = 0; i < 10; i++) {
      DataProductAssociation association = new DataProductAssociation();
      association.setDestinationUrn(
          UrnUtils.getUrn(
              String.format("urn:li:dataset:(urn:li:dataPlatform:hive,table_%d,PROD)", i)));
      associations.add(association);
    }
    DataProductProperties props = new DataProductProperties();
    props.setAssets(associations);

    ChangeItemImpl change = changeItem(props, ChangeType.CREATE);
    DataProductAssetsSideEffect test = new DataProductAssetsSideEffect();
    test.setConfig(CONFIG);
    test.setMaxFanoutPerCommit(3);

    List<MCPItem> output =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(change, null, null, retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(output.size(), 3, "Fan-out should be capped at maxFanoutPerCommit");
  }
}
