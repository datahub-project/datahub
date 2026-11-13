package com.linkedin.metadata.entity.ebean.batch;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.patch.builder.MLModelPropertiesPatchBuilder;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.ml.metadata.MLHyperParam;
import com.linkedin.ml.metadata.MLHyperParamArray;
import com.linkedin.ml.metadata.MLMetric;
import com.linkedin.ml.metadata.MLMetricArray;
import com.linkedin.ml.metadata.MLModelProperties;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * PATCH of {@code mlModelProperties} through {@link PatchItemImpl}, using the MCP that {@link
 * MLModelPropertiesPatchBuilder} (Java SDK v2 {@code MLModel}) produces.
 */
public class MLModelPropertiesPatchItemImplTest {

  private static final Urn MODEL_URN =
      UrnUtils.getUrn("urn:li:mlModel:(urn:li:dataPlatform:sagemaker,my_model,PROD)");

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final EntityRegistry entityRegistry = opContext.getEntityRegistry();
  private final AspectRetriever aspectRetriever = opContext.getAspectRetriever();
  private final AuditStamp auditStamp =
      new AuditStamp().setTime(1000L).setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"));

  private PatchItemImpl patchItem(MLModelPropertiesPatchBuilder builder) {
    return PatchItemImpl.builder()
        .build(builder.urn(MODEL_URN).build(), auditStamp, entityRegistry);
  }

  @Test
  public void testPatchWithNoExistingAspect() throws Exception {
    PatchItemImpl item =
        patchItem(
            new MLModelPropertiesPatchBuilder()
                .setDescription("churn model")
                .setExternalUrl("https://example.com/model")
                .addCustomProperty("framework", "xgboost")
                .addTrainingMetric("accuracy", "0.91")
                .addHyperParam("max_depth", "6"));

    MLModelProperties result =
        (MLModelProperties) item.applyPatch(null, aspectRetriever).getRecordTemplate();

    assertEquals(result.getDescription(), "churn model");
    assertEquals(result.getExternalUrl(), new Url("https://example.com/model"));
    assertEquals(result.getCustomProperties(), new StringMap(Map.of("framework", "xgboost")));
    assertEquals(
        result.getTrainingMetrics(),
        new MLMetricArray(List.of(new MLMetric().setName("accuracy").setValue("0.91"))));
    assertEquals(
        result.getHyperParams(),
        new MLHyperParamArray(List.of(new MLHyperParam().setName("max_depth").setValue("6"))));
  }

  @Test
  public void testPatchWithExistingAspect() {
    MLModelProperties existing =
        new MLModelProperties()
            .setName("my_model")
            .setDescription("old description")
            .setCustomProperties(new StringMap(Map.of("owner_team", "ml")))
            .setTrainingMetrics(
                new MLMetricArray(List.of(new MLMetric().setName("accuracy").setValue("0.80"))));

    PatchItemImpl item =
        patchItem(
            new MLModelPropertiesPatchBuilder()
                .setDescription("new description")
                .addCustomProperty("framework", "xgboost")
                .addTrainingMetric("f1", "0.88"));

    MLModelProperties result =
        (MLModelProperties) item.applyPatch(existing, aspectRetriever).getRecordTemplate();

    assertEquals(result.getName(), "my_model");
    assertEquals(result.getDescription(), "new description");
    assertEquals(
        result.getCustomProperties(),
        new StringMap(Map.of("owner_team", "ml", "framework", "xgboost")));
    assertEquals(
        result.getTrainingMetrics(),
        new MLMetricArray(
            List.of(
                new MLMetric().setName("accuracy").setValue("0.80"),
                new MLMetric().setName("f1").setValue("0.88"))));
  }
}
