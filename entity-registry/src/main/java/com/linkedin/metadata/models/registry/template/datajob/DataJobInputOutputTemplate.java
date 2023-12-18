package com.linkedin.metadata.models.registry.template.datajob;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.DataJobUrnArray;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.metadata.models.registry.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class DataJobInputOutputTemplate implements ArrayMergingTemplate<DataJobInputOutput> {

  private static final String INPUT_DATA_JOB_EDGES_FIELD_NAME = "inputDatajobEdges";
  private static final String INPUT_DATASET_EDGES_FIELD_NAME = "inputDatasetEdges";
  private static final String OUTPUT_DATASET_EDGES_FIELD_NAME = "outputDatasetEdges";

  private static final String DESTINATION_URN_FIELD_NAME = "destinationUrn";

  private static final String INPUT_DATASET_FIELDS_FIELD_NAME = "inputDatasetFields";
  private static final String OUTPUT_DATASET_FIELDS_FIELD_NAME = "outputDatasetFields";

  // TODO: Fine Grained Lineages not patchable at this time, they don't have a well established key

  @Override
  public DataJobInputOutput getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof DataJobInputOutput) {
      return (DataJobInputOutput) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DataJobInputOutput");
  }

  @Override
  public Class<DataJobInputOutput> getTemplateType() {
    return DataJobInputOutput.class;
  }

  @Nonnull
  @Override
  public DataJobInputOutput getDefault() {
    DataJobInputOutput dataJobInputOutput = new DataJobInputOutput();
    dataJobInputOutput.setInputDatajobEdges(new EdgeArray());
    dataJobInputOutput.setInputDatasetEdges(new EdgeArray());
    dataJobInputOutput.setOutputDatasetEdges(new EdgeArray());
    dataJobInputOutput.setFineGrainedLineages(new FineGrainedLineageArray());
    dataJobInputOutput.setInputDatasetFields(new UrnArray());
    dataJobInputOutput.setOutputDatasetFields(new UrnArray());

    // Deprecated fields
    dataJobInputOutput.setInputDatajobs(new DataJobUrnArray());
    dataJobInputOutput.setInputDatasets(new DatasetUrnArray());
    dataJobInputOutput.setOutputDatasets(new DatasetUrnArray());

    return dataJobInputOutput;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode,
            INPUT_DATA_JOB_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    transformedNode =
        arrayFieldToMap(
            transformedNode,
            INPUT_DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    transformedNode =
        arrayFieldToMap(
            transformedNode,
            OUTPUT_DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    transformedNode =
        arrayFieldToMap(transformedNode, INPUT_DATASET_FIELDS_FIELD_NAME, Collections.emptyList());
    transformedNode =
        arrayFieldToMap(transformedNode, OUTPUT_DATASET_FIELDS_FIELD_NAME, Collections.emptyList());

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched,
            INPUT_DATA_JOB_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    rebasedNode =
        transformedMapToArray(
            rebasedNode,
            INPUT_DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    rebasedNode =
        transformedMapToArray(
            rebasedNode,
            OUTPUT_DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    rebasedNode =
        transformedMapToArray(
            rebasedNode, INPUT_DATASET_FIELDS_FIELD_NAME, Collections.emptyList());
    rebasedNode =
        transformedMapToArray(
            rebasedNode, OUTPUT_DATASET_FIELDS_FIELD_NAME, Collections.emptyList());

    return rebasedNode;
  }
}
