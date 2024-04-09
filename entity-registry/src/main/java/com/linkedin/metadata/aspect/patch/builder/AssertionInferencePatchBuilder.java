package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.ASSERTION_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_INFERENCE_DETAILS_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class AssertionInferencePatchBuilder
    extends AbstractMultiFieldPatchBuilder<AssertionInferencePatchBuilder> {

  public static final String BASE_PATH = "/";
  public static final String MODEL_ID_KEY = "modelId";
  public static final String CONFIDENCE_KEY = "confidence";
  public static final String GENERATED_AT_KEY = "generatedAt";

  public AssertionInferencePatchBuilder setModelId(@Nonnull String modelId) {
    TextNode modelIdNode = instance.textNode(modelId);
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + MODEL_ID_KEY, modelIdNode));
    return this;
  }

  public AssertionInferencePatchBuilder setConfidence(@Nonnull Float confidence) {
    ValueNode confidenceNode = instance.numberNode(confidence);
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + CONFIDENCE_KEY, confidenceNode));
    return this;
  }

  public AssertionInferencePatchBuilder setGeneratedAt(@Nonnull Long generatedAt) {
    ValueNode generatedAtNode = instance.numberNode(generatedAt);
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + GENERATED_AT_KEY, generatedAtNode));
    return this;
  }

  @Override
  protected String getAspectName() {
    return ASSERTION_INFERENCE_DETAILS_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return ASSERTION_ENTITY_NAME;
  }
}
