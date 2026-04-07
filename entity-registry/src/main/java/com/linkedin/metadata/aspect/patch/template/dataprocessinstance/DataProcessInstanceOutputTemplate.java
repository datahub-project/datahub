// ABOUTME: Template for PATCH operations on DataProcessInstanceOutput aspect.
// ABOUTME: Transforms outputs (URN array) and outputEdges (Edge array) for JSON patch
// compatibility.
package com.linkedin.metadata.aspect.patch.template.dataprocessinstance;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class DataProcessInstanceOutputTemplate
    implements ArrayMergingTemplate<DataProcessInstanceOutput> {

  private static final String OUTPUTS_FIELD_NAME = "outputs";
  private static final String OUTPUT_EDGES_FIELD_NAME = "outputEdges";
  private static final String DESTINATION_URN_FIELD_NAME = "destinationUrn";

  @Override
  public DataProcessInstanceOutput getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof DataProcessInstanceOutput) {
      return (DataProcessInstanceOutput) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DataProcessInstanceOutput");
  }

  @Override
  public Class<DataProcessInstanceOutput> getTemplateType() {
    return DataProcessInstanceOutput.class;
  }

  @Nonnull
  @Override
  public DataProcessInstanceOutput getDefault() {
    DataProcessInstanceOutput output = new DataProcessInstanceOutput();
    output.setOutputs(new UrnArray());
    output.setOutputEdges(new EdgeArray());
    return output;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(baseNode, OUTPUTS_FIELD_NAME, Collections.emptyList());

    transformedNode =
        arrayFieldToMap(
            transformedNode,
            OUTPUT_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(patched, OUTPUTS_FIELD_NAME, Collections.emptyList());

    rebasedNode =
        transformedMapToArray(
            rebasedNode,
            OUTPUT_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    return rebasedNode;
  }
}
