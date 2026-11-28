package com.linkedin.metadata.utils;

import static org.testng.Assert.assertThrows;

import com.linkedin.common.FabricTypeNames;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatforminstance.IcebergWarehouseInfo;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

/**
 * FabricTypeName fields keep the shape check the FabricType enum used to give, both in URN keys and
 * in ordinary aspect fields. Existence of custom names is checked elsewhere.
 */
public class FabricTypeNameValidationTest {

  private static final EntityRegistry ENTITY_REGISTRY =
      TestOperationContexts.defaultEntityRegistry();

  @Test
  public void testDatasetUrnEnvShape() {
    validateUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.tbl,PROD)");
    validateUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.tbl,MY_ENV)");

    assertThrows(
        IllegalArgumentException.class,
        () -> validateUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.tbl,prod)"));
    assertThrows(
        IllegalArgumentException.class,
        () -> validateUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.tbl,MY-ENV)"));
  }

  @Test
  public void testAspectEnvShape() {
    validateAspect(icebergWarehouseInfo("DEV"));
    validateAspect(icebergWarehouseInfo("MY_ENV"));
    assertThrows(IllegalStateException.class, () -> validateAspect(icebergWarehouseInfo("dev")));
  }

  @Test
  public void testDefaultValidatorChecksShape() {
    RecordTemplateValidator.validate(datasetKey("MY_ENV"), FabricTypeNameValidationTest::fail);
    assertThrows(
        IllegalStateException.class,
        () ->
            RecordTemplateValidator.validate(
                datasetKey("my_env"), FabricTypeNameValidationTest::fail));
  }

  private static void validateUrn(String urn) {
    UrnValidationUtil.validateUrn(ENTITY_REGISTRY, UrnUtils.getUrn(urn), true);
  }

  private static void validateAspect(RecordTemplate aspect) {
    EntityRegistryUrnValidator validator = new EntityRegistryUrnValidator(ENTITY_REGISTRY);
    validator.setCurrentEntitySpec(ENTITY_REGISTRY.getEntitySpec("dataPlatformInstance"));
    RecordTemplateValidator.validate(aspect, FabricTypeNameValidationTest::fail, validator);
  }

  private static void fail(Object result) {
    throw new IllegalStateException(String.valueOf(result));
  }

  private static DatasetKey datasetKey(String origin) {
    DataMap data = new DataMap();
    data.put("platform", "urn:li:dataPlatform:hive");
    data.put("name", "db.tbl");
    data.put("origin", origin);
    return new DatasetKey(data);
  }

  private static IcebergWarehouseInfo icebergWarehouseInfo(String env) {
    IcebergWarehouseInfo info =
        new IcebergWarehouseInfo()
            .setDataRoot("s3://bucket/wh")
            .setClientId(UrnUtils.getUrn("urn:li:dataHubSecret:id"))
            .setClientSecret(UrnUtils.getUrn("urn:li:dataHubSecret:secret"))
            .setRegion("us-east-1");
    FabricTypeNames.set(info, "env", env);
    return info;
  }
}
