package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.Constants.ML_MODEL_PROPERTIES_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Status;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.patch.builder.MLModelPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.template.ml.MLModelPropertiesTemplate;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.ml.metadata.MLHyperParam;
import com.linkedin.ml.metadata.MLHyperParamArray;
import com.linkedin.ml.metadata.MLMetric;
import com.linkedin.ml.metadata.MLMetricArray;
import com.linkedin.ml.metadata.MLModelProperties;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Applies the exact operations emitted by {@link MLModelPropertiesPatchBuilder} (used by the Java
 * SDK v2 {@code MLModel}) to {@link MLModelPropertiesTemplate}.
 */
public class MLModelPropertiesTemplateTest {

  private static final Urn MODEL_URN =
      UrnUtils.getUrn("urn:li:mlModel:(urn:li:dataPlatform:sagemaker,my_model,PROD)");

  private final MLModelPropertiesTemplate template = new MLModelPropertiesTemplate();

  private static JsonPatch toJsonPatch(MLModelPropertiesPatchBuilder builder) {
    String payload =
        builder.urn(MODEL_URN).build().getAspect().getValue().asString(StandardCharsets.UTF_8);
    return Json.createPatch(Json.createReader(new StringReader(payload)).readArray());
  }

  private static MLMetric metric(String name, String value) {
    return new MLMetric().setName(name).setValue(value);
  }

  private static MLHyperParam hyperParam(String name, String value) {
    return new MLHyperParam().setName(name).setValue(value);
  }

  @Test
  public void testGetDefault() {
    MLModelProperties defaultProperties = template.getDefault();
    // Written explicitly so a patch-created aspect stores customProperties like ingestion does.
    assertTrue(defaultProperties.hasCustomProperties());
    assertTrue(defaultProperties.getCustomProperties().isEmpty());
    assertFalse(defaultProperties.hasDescription());
    assertFalse(defaultProperties.hasTrainingMetrics());
    assertFalse(defaultProperties.hasHyperParams());
  }

  @Test
  public void testGetTemplateType() {
    assertEquals(template.getTemplateType(), MLModelProperties.class);
  }

  @Test
  public void testGetSubtypeRejectsForeignRecordTemplate() {
    assertThrows(ClassCastException.class, () -> template.getSubtype(new Status()));
  }

  @Test
  public void testTemplateIsRegisteredInSnapshotEntityRegistry() {
    RecordTemplate defaultTemplate =
        new SnapshotEntityRegistry()
            .getAspectTemplateEngine()
            .getDefaultTemplate(ML_MODEL_PROPERTIES_ASPECT_NAME);
    assertTrue(defaultTemplate instanceof MLModelProperties);
  }

  @Test
  public void testEveryBuilderOperationOntoDefault() throws Exception {
    JsonPatch patch =
        toJsonPatch(
            new MLModelPropertiesPatchBuilder()
                .setDescription("churn model")
                .setExternalUrl("https://example.com/model")
                .addCustomProperty("framework", "xgboost")
                .addTrainingMetric("accuracy", "0.91")
                .addTrainingMetric("f1", "0.88")
                .addHyperParam("max_depth", "6"));

    MLModelProperties result = template.applyPatch(template.getDefault(), patch);

    assertEquals(result.getDescription(), "churn model");
    assertEquals(result.getExternalUrl(), new Url("https://example.com/model"));
    assertEquals(result.getCustomProperties(), new StringMap(Map.of("framework", "xgboost")));
    assertEquals(
        result.getTrainingMetrics(),
        new MLMetricArray(List.of(metric("accuracy", "0.91"), metric("f1", "0.88"))));
    assertEquals(
        result.getHyperParams(), new MLHyperParamArray(List.of(hyperParam("max_depth", "6"))));
  }

  @Test
  public void testAppendsToExistingArraysAndKeepsOtherFields() throws Exception {
    MLModelProperties existing = template.getDefault();
    existing.setName("my_model");
    existing.setDescription("old description");
    existing.setCustomProperties(new StringMap(Map.of("owner_team", "ml", "framework", "sklearn")));
    existing.setTrainingMetrics(new MLMetricArray(List.of(metric("accuracy", "0.80"))));
    existing.setHyperParams(new MLHyperParamArray(List.of(hyperParam("max_depth", "4"))));

    JsonPatch patch =
        toJsonPatch(
            new MLModelPropertiesPatchBuilder()
                .addCustomProperty("framework", "xgboost")
                .addTrainingMetric("accuracy", "0.91")
                .addHyperParam("eta", "0.3"));

    MLModelProperties result = template.applyPatch(existing, patch);

    assertEquals(result.getName(), "my_model");
    assertEquals(result.getDescription(), "old description");
    assertEquals(
        result.getCustomProperties(),
        new StringMap(Map.of("owner_team", "ml", "framework", "xgboost")));
    assertEquals(
        result.getTrainingMetrics(),
        new MLMetricArray(List.of(metric("accuracy", "0.80"), metric("accuracy", "0.91"))));
    assertEquals(
        result.getHyperParams(),
        new MLHyperParamArray(List.of(hyperParam("max_depth", "4"), hyperParam("eta", "0.3"))));
  }

  @Test
  public void testRemoveOperations() throws Exception {
    MLModelProperties existing = template.getDefault();
    existing.setDescription("old description");
    existing.setExternalUrl(new Url("https://example.com/old"));
    existing.setCustomProperties(new StringMap(Map.of("owner_team", "ml", "framework", "sklearn")));

    JsonPatch patch =
        toJsonPatch(
            new MLModelPropertiesPatchBuilder()
                .setDescription(null)
                .setExternalUrl(null)
                .removeCustomProperty("framework"));

    MLModelProperties result = template.applyPatch(existing, patch);

    assertFalse(result.hasDescription());
    assertFalse(result.hasExternalUrl());
    assertEquals(result.getCustomProperties(), new StringMap(Map.of("owner_team", "ml")));
  }

  @Test
  public void testSetCustomPropertiesReplacesMap() throws Exception {
    MLModelProperties existing = template.getDefault();
    existing.setCustomProperties(new StringMap(Map.of("owner_team", "ml")));

    JsonPatch patch =
        toJsonPatch(
            new MLModelPropertiesPatchBuilder()
                .setCustomProperties(Map.of("framework", "xgboost")));

    MLModelProperties result = template.applyPatch(existing, patch);

    assertEquals(result.getCustomProperties(), new StringMap(Map.of("framework", "xgboost")));
  }

  @Test
  public void testUnrelatedPatchDoesNotMaterializeEmptyArrays() throws Exception {
    MLModelProperties existing = template.getDefault();
    existing.setName("my_model");

    MLModelProperties result =
        template.applyPatch(
            existing, toJsonPatch(new MLModelPropertiesPatchBuilder().setDescription("desc")));

    assertEquals(result.getDescription(), "desc");
    assertFalse(result.hasTrainingMetrics());
    assertFalse(result.hasHyperParams());
  }

  @Test
  public void testUnrelatedPatchKeepsStoredEmptyArrays() throws Exception {
    MLModelProperties existing = template.getDefault();
    existing.setTrainingMetrics(new MLMetricArray());
    existing.setHyperParams(new MLHyperParamArray());

    MLModelProperties result =
        template.applyPatch(
            existing, toJsonPatch(new MLModelPropertiesPatchBuilder().setDescription("desc")));

    assertEquals(result.getTrainingMetrics(), new MLMetricArray());
    assertEquals(result.getHyperParams(), new MLHyperParamArray());
  }
}
