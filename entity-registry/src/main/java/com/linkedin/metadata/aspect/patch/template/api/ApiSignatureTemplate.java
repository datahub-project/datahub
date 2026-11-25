package com.linkedin.metadata.aspect.patch.template.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.api.ApiSignature;
import com.linkedin.common.EdgeArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Patch template for {@code apiSignature}. The dataset edges are keyed by {@code destinationUrn} so
 * a caller can add or remove a single input/output dataset reference without rewriting the rest of
 * the signature (the inline fields, or edges written by another producer). Mirrors {@link
 * com.linkedin.metadata.aspect.patch.template.datajob.DataJobInputOutputTemplate}.
 */
public class ApiSignatureTemplate implements ArrayMergingTemplate<ApiSignature> {

  private static final String INPUT_DATASET_EDGES_FIELD_NAME = "inputDatasetEdges";
  private static final String OUTPUT_DATASET_EDGES_FIELD_NAME = "outputDatasetEdges";
  private static final String DESTINATION_URN_FIELD_NAME = "destinationUrn";

  @Override
  public ApiSignature getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof ApiSignature) {
      return (ApiSignature) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to ApiSignature");
  }

  @Override
  public Class<ApiSignature> getTemplateType() {
    return ApiSignature.class;
  }

  @Nonnull
  @Override
  public ApiSignature getDefault() {
    final ApiSignature apiSignature = new ApiSignature();
    apiSignature.setInputDatasetEdges(new EdgeArray());
    apiSignature.setOutputDatasetEdges(new EdgeArray());
    return apiSignature;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode,
            INPUT_DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));
    transformedNode =
        arrayFieldToMap(
            transformedNode,
            OUTPUT_DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));
    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched,
            INPUT_DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));
    rebasedNode =
        transformedMapToArray(
            rebasedNode,
            OUTPUT_DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));
    return rebasedNode;
  }
}
