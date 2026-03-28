// ABOUTME: Template for PATCH operations on DataProcessInstanceInput aspect.
// ABOUTME: Transforms inputs (URN array) and inputEdges (Edge array) for JSON patch compatibility.
package com.linkedin.metadata.aspect.patch.template.dataprocessinstance;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class DataProcessInstanceInputTemplate
    implements ArrayMergingTemplate<DataProcessInstanceInput> {

  private static final String INPUTS_FIELD_NAME = "inputs";
  private static final String INPUT_EDGES_FIELD_NAME = "inputEdges";
  private static final String DESTINATION_URN_FIELD_NAME = "destinationUrn";

  @Override
  public DataProcessInstanceInput getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof DataProcessInstanceInput) {
      return (DataProcessInstanceInput) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DataProcessInstanceInput");
  }

  @Override
  public Class<DataProcessInstanceInput> getTemplateType() {
    return DataProcessInstanceInput.class;
  }

  @Nonnull
  @Override
  public DataProcessInstanceInput getDefault() {
    DataProcessInstanceInput input = new DataProcessInstanceInput();
    input.setInputs(new UrnArray());
    input.setInputEdges(new EdgeArray());
    return input;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(baseNode, INPUTS_FIELD_NAME, Collections.emptyList());

    transformedNode =
        arrayFieldToMap(
            transformedNode,
            INPUT_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(patched, INPUTS_FIELD_NAME, Collections.emptyList());

    rebasedNode =
        transformedMapToArray(
            rebasedNode,
            INPUT_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    return rebasedNode;
  }
}
