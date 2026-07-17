package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.ML_MODEL_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_MODEL_PROPERTIES_ASPECT_NAME;

import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.builder.subtypesupport.CustomPropertiesPatchBuilderSupport;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class MLModelPropertiesPatchBuilder
    extends AbstractMultiFieldPatchBuilder<MLModelPropertiesPatchBuilder>
    implements CustomPropertiesPatchBuilderSupport<MLModelPropertiesPatchBuilder> {

  public static final String BASE_PATH = "/";

  public static final String DESCRIPTION_KEY = "description";
  public static final String EXTERNAL_URL_KEY = "externalUrl";

  private CustomPropertiesPatchBuilder<MLModelPropertiesPatchBuilder> customPropertiesPatchBuilder =
      new CustomPropertiesPatchBuilder<>(this);

  public MLModelPropertiesPatchBuilder setDescription(@Nullable String description) {
    if (description == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + DESCRIPTION_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + DESCRIPTION_KEY,
              instance.textNode(description)));
    }
    return this;
  }

  public MLModelPropertiesPatchBuilder setExternalUrl(@Nullable String externalUrl) {
    if (externalUrl == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + EXTERNAL_URL_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + EXTERNAL_URL_KEY,
              instance.textNode(externalUrl)));
    }
    return this;
  }

  @Override
  public MLModelPropertiesPatchBuilder addCustomProperty(
      @Nonnull String key, @Nonnull String value) {
    this.customPropertiesPatchBuilder.addProperty(key, value);
    return this;
  }

  @Override
  public MLModelPropertiesPatchBuilder removeCustomProperty(@Nonnull String key) {
    this.customPropertiesPatchBuilder.removeProperty(key);
    return this;
  }

  @Override
  public MLModelPropertiesPatchBuilder setCustomProperties(Map<String, String> properties) {
    customPropertiesPatchBuilder.setProperties(properties);
    return this;
  }

  public MLModelPropertiesPatchBuilder addTrainingMetric(
      @Nonnull String name, @Nonnull String value) {
    com.fasterxml.jackson.databind.node.ObjectNode metricNode = instance.objectNode();
    metricNode.put("name", name);
    metricNode.put("value", value);
    this.pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), "/trainingMetrics/-", metricNode));
    return this;
  }

  public MLModelPropertiesPatchBuilder addHyperParam(@Nonnull String name, @Nonnull String value) {
    com.fasterxml.jackson.databind.node.ObjectNode hyperParamNode = instance.objectNode();
    hyperParamNode.put("name", name);
    hyperParamNode.put("value", value);
    this.pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), "/hyperParams/-", hyperParamNode));
    return this;
  }

  @Override
  protected String getAspectName() {
    return ML_MODEL_PROPERTIES_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return ML_MODEL_ENTITY_NAME;
  }
}
