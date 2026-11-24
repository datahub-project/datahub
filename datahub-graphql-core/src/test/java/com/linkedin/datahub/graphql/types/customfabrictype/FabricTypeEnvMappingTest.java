package com.linkedin.datahub.graphql.types.customfabrictype;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.common.FabricTypeNames;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.VersionedDataset;
import com.linkedin.datahub.graphql.types.common.mappers.FabricTypeMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.VersionedDatasetMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLModelGroupMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLModelMapper;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.MLModelGroupKey;
import com.linkedin.metadata.key.MLModelKey;
import java.util.Arrays;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * C1: for every built-in env, origin maps to the same GraphQL FabricType as before and env holds
 * its name. A custom env maps to origin OTHER, with the actual value in env.
 */
public class FabricTypeEnvMappingTest {

  private static final Urn HIVE = UrnUtils.getUrn("urn:li:dataPlatform:hive");

  @Test
  public void testEveryBuiltInMapsToTheSameGraphQLSymbol() {
    for (com.linkedin.common.FabricType env : builtIns()) {
      FabricType expected = FabricType.valueOf(env.name());

      Dataset dataset = DatasetMapper.map(null, datasetResponse(env.name(), true));
      assertSame(dataset.getOrigin(), expected);
      assertEquals(dataset.getEnv(), env.name());
      assertSame(dataset.getProperties().getOrigin(), expected);
      assertEquals(dataset.getProperties().getEnv(), env.name());

      VersionedDataset versioned =
          VersionedDatasetMapper.map(null, datasetResponse(env.name(), false));
      assertSame(versioned.getOrigin(), expected);
      assertEquals(versioned.getEnv(), env.name());

      MLModel model = MLModelMapper.map(null, modelResponse(env.name()));
      assertSame(model.getOrigin(), expected);
      assertEquals(model.getEnv(), env.name());

      MLModelGroup group = MLModelGroupMapper.map(null, groupResponse(env.name()));
      assertSame(group.getOrigin(), expected);
      assertEquals(group.getEnv(), env.name());
    }
  }

  @Test
  public void testCustomEnvMapsToOther() {
    Dataset dataset = DatasetMapper.map(null, datasetResponse("MY_ENV", true));
    assertSame(dataset.getOrigin(), FabricType.OTHER);
    assertEquals(dataset.getEnv(), "MY_ENV");
    assertSame(dataset.getProperties().getOrigin(), FabricType.OTHER);
    assertEquals(dataset.getProperties().getEnv(), "MY_ENV");

    assertSame(MLModelMapper.map(null, modelResponse("MY_ENV")).getOrigin(), FabricType.OTHER);
    assertEquals(MLModelGroupMapper.map(null, groupResponse("MY_ENV")).getEnv(), "MY_ENV");
  }

  @Test
  public void testCustomFabricTypeFieldLoadsOnlyForCustomEnvs() {
    assertEquals(FabricTypeMapper.customFabricTypeUrn("MY_ENV"), "urn:li:customFabricType:MY_ENV");
    assertNull(FabricTypeMapper.customFabricTypeUrn("PROD"));
    assertNull(FabricTypeMapper.customFabricTypeUrn(null));
  }

  private static EntityResponse datasetResponse(String env, boolean withProperties) {
    DatasetKey key = new DatasetKey().setPlatform(HIVE).setName("db.t");
    FabricTypeNames.set(key, "origin", env);
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.DATASET_KEY_ASPECT_NAME, enveloped(key));
    if (withProperties) {
      aspects.put(
          Constants.DATASET_PROPERTIES_ASPECT_NAME,
          enveloped(new DatasetProperties().setName("t")));
    }
    return response(
        Constants.DATASET_ENTITY_NAME,
        "urn:li:dataset:(urn:li:dataPlatform:hive,db.t," + env + ")",
        aspects);
  }

  private static EntityResponse modelResponse(String env) {
    MLModelKey key = new MLModelKey().setPlatform(HIVE).setName("m");
    FabricTypeNames.set(key, "origin", env);
    return response(
        Constants.ML_MODEL_ENTITY_NAME,
        "urn:li:mlModel:(urn:li:dataPlatform:hive,m," + env + ")",
        new EnvelopedAspectMap(Map.of(Constants.ML_MODEL_KEY_ASPECT_NAME, enveloped(key))));
  }

  private static EntityResponse groupResponse(String env) {
    MLModelGroupKey key = new MLModelGroupKey().setPlatform(HIVE).setName("g");
    FabricTypeNames.set(key, "origin", env);
    return response(
        Constants.ML_MODEL_GROUP_ENTITY_NAME,
        "urn:li:mlModelGroup:(urn:li:dataPlatform:hive,g," + env + ")",
        new EnvelopedAspectMap(Map.of(Constants.ML_MODEL_GROUP_KEY_ASPECT_NAME, enveloped(key))));
  }

  private static EntityResponse response(
      String entityName, String urn, EnvelopedAspectMap aspects) {
    return new EntityResponse()
        .setEntityName(entityName)
        .setUrn(UrnUtils.getUrn(urn))
        .setAspects(aspects);
  }

  private static EnvelopedAspect enveloped(RecordTemplate aspect) {
    return new EnvelopedAspect().setValue(new Aspect(aspect.data()));
  }

  private static com.linkedin.common.FabricType[] builtIns() {
    return Arrays.stream(com.linkedin.common.FabricType.values())
        .filter(env -> env != com.linkedin.common.FabricType.$UNKNOWN)
        .toArray(com.linkedin.common.FabricType[]::new);
  }
}
