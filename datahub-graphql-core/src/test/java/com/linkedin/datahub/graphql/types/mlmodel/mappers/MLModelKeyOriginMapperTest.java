package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import java.util.Map;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Key aspects whose origin holds a FabricType symbol only a newer version defines. */
public class MLModelKeyOriginMapperTest {

  @BeforeMethod
  public void tolerateUnknownEnums() {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @Test
  public void testMLModelKeyWithOriginFromNewerVersion() {
    final MLModel actual =
        MLModelMapper.map(
            null,
            response(
                Constants.ML_MODEL_ENTITY_NAME,
                "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,my_model,PROD)",
                Constants.ML_MODEL_KEY_ASPECT_NAME));

    assertEquals(actual.getName(), "my_model");
    assertEquals(actual.getOrigin(), FabricType.PROD);
  }

  @Test
  public void testMLModelGroupKeyWithOriginFromNewerVersion() {
    final MLModelGroup actual =
        MLModelGroupMapper.map(
            null,
            response(
                Constants.ML_MODEL_GROUP_ENTITY_NAME,
                "urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,my_model,PROD)",
                Constants.ML_MODEL_GROUP_KEY_ASPECT_NAME));

    assertEquals(actual.getName(), "my_model");
    assertEquals(actual.getOrigin(), FabricType.PROD);
  }

  private static EntityResponse response(
      final String entityName, final String urn, final String keyAspectName) {
    final DataMap key = new DataMap();
    key.put("platform", "urn:li:dataPlatform:sagemaker");
    key.put("name", "my_model");
    key.put("origin", "ENV_FROM_NEWER_VERSION");
    return new EntityResponse()
        .setEntityName(entityName)
        .setUrn(UrnUtils.getUrn(urn))
        .setAspects(
            new EnvelopedAspectMap(
                Map.of(keyAspectName, new EnvelopedAspect().setValue(new Aspect(key)))));
  }
}
