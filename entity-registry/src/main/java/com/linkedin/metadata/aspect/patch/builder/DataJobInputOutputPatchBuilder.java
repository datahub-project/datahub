package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME;
import static com.linkedin.metadata.aspect.patch.builder.PatchUtil.createEdgeValue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.graph.LineageDirection;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DataJobInputOutputPatchBuilder
    extends AbstractMultiFieldPatchBuilder<DataJobInputOutputPatchBuilder> {
  private static final String INPUT_DATA_JOB_EDGES_PATH_START = "/inputDatajobEdges/";
  private static final String INPUT_DATASET_EDGES_PATH_START = "/inputDatasetEdges/";
  private static final String OUTPUT_DATASET_EDGES_PATH_START = "/outputDatasetEdges/";
  private static final String INPUT_DATASET_FIELDS_PATH_START = "/inputDatasetFields/";
  private static final String OUTPUT_DATASET_FIELDS_PATH_START = "/outputDatasetFields/";

  // Simplified with just Urn
  public DataJobInputOutputPatchBuilder addInputDatajobEdge(@Nonnull DataJobUrn dataJobUrn) {
    ObjectNode value = createEdgeValue(dataJobUrn);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            INPUT_DATA_JOB_EDGES_PATH_START + encodeValue(dataJobUrn.toString()),
            value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeInputDatajobEdge(@Nonnull DataJobUrn dataJobUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            INPUT_DATA_JOB_EDGES_PATH_START + encodeValue(dataJobUrn.toString()),
            null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addInputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    ObjectNode value = createEdgeValue(datasetUrn);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            INPUT_DATASET_EDGES_PATH_START + encodeValue(datasetUrn.toString()),
            value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeInputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            INPUT_DATASET_EDGES_PATH_START + encodeValue(datasetUrn.toString()),
            null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addOutputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    ObjectNode value = createEdgeValue(datasetUrn);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            OUTPUT_DATASET_EDGES_PATH_START + encodeValue(datasetUrn.toString()),
            value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeOutputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            OUTPUT_DATASET_EDGES_PATH_START + encodeValue(datasetUrn.toString()),
            null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addInputDatasetField(@Nonnull Urn urn) {
    TextNode textNode = instance.textNode(urn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            INPUT_DATASET_FIELDS_PATH_START + encodeValue(urn.toString()),
            textNode));

    return this;
  }

  public DataJobInputOutputPatchBuilder removeInputDatasetField(@Nonnull Urn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            INPUT_DATASET_FIELDS_PATH_START + encodeValue(urn.toString()),
            null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addOutputDatasetField(@Nonnull Urn urn) {
    TextNode textNode = instance.textNode(urn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            OUTPUT_DATASET_FIELDS_PATH_START + encodeValue(urn.toString()),
            textNode));

    return this;
  }

  public DataJobInputOutputPatchBuilder removeOutputDatasetField(@Nonnull Urn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            OUTPUT_DATASET_FIELDS_PATH_START + encodeValue(urn.toString()),
            null));
    return this;
  }

  // Full Edge modification
  public DataJobInputOutputPatchBuilder addEdge(
      @Nonnull Edge edge, @Nonnull LineageDirection direction) {
    ObjectNode value = createEdgeValue(edge);
    String path = getEdgePath(edge, direction);

    pathValues.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), path, value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeEdge(
      @Nonnull Edge edge, @Nonnull LineageDirection direction) {
    String path = getEdgePath(edge, direction);

    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), path, null));
    return this;
  }

  /**
   * Determines Edge path based on supplied Urn, if not a valid entity type throws
   * IllegalArgumentException
   *
   * @param edge
   * @return
   * @throws IllegalArgumentException if destinationUrn is an invalid entity type
   */
  private String getEdgePath(@Nonnull Edge edge, LineageDirection direction) {
    Urn destinationUrn = edge.getDestinationUrn();

    if (DATASET_ENTITY_NAME.equals(destinationUrn.getEntityType())
        && LineageDirection.UPSTREAM.equals(direction)) {
      return INPUT_DATASET_EDGES_PATH_START + encodeValue(destinationUrn.toString());
    }

    if (DATASET_ENTITY_NAME.equals(destinationUrn.getEntityType())
        && LineageDirection.DOWNSTREAM.equals(direction)) {
      return INPUT_DATASET_EDGES_PATH_START + encodeValue(destinationUrn.toString());
    }

    if (DATA_JOB_ENTITY_NAME.equals(destinationUrn.getEntityType())
        && LineageDirection.UPSTREAM.equals(direction)) {
      return INPUT_DATA_JOB_EDGES_PATH_START + encodeValue(destinationUrn.toString());
    }

    // TODO: Output Data Jobs not supported by aspect, add here if this changes

    throw new IllegalArgumentException(
        String.format("Unsupported entity type: %s", destinationUrn.getEntityType()));
  }

  @Override
  protected String getAspectName() {
    return DATA_JOB_INPUT_OUTPUT_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATA_JOB_ENTITY_NAME;
  }
}
