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
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DataJobInputOutputPatchBuilder
    extends AbstractMultiFieldPatchBuilder<DataJobInputOutputPatchBuilder> {
  private static final String INPUT_DATA_JOB_EDGES_PATH_START = "/inputDatajobEdges/";
  private static final String INPUT_DATASET_EDGES_PATH_START = "/inputDatasetEdges/";
  private static final String OUTPUT_DATASET_EDGES_PATH_START = "/outputDatasetEdges/";
  private static final String INPUT_DATASET_FIELDS_PATH_START = "/inputDatasetFields/";
  private static final String OUTPUT_DATASET_FIELDS_PATH_START = "/outputDatasetFields/";
  private static final String FINE_GRAINED_PATH_START = "/fineGrainedLineages/";

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

  /**
   * Adds a field as a fine grained upstream
   *
   * @param upstreamSchemaField a schema field to be marked as upstream, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param confidenceScore optional, confidence score for the lineage edge. Defaults to 1.0 for
   *     full confidence
   * @param transformationOperation string operation type that describes the transformation
   *     operation happening in the lineage edge
   * @param downstreamSchemaField the downstream schema field this upstream is derived from, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param queryUrn query urn the relationship is derived from
   * @return this builder
   */
  public DataJobInputOutputPatchBuilder addFineGrainedUpstreamField(
      @Nonnull Urn upstreamSchemaField,
      @Nullable Float confidenceScore,
      @Nonnull String transformationOperation,
      @Nonnull Urn downstreamSchemaField,
      @Nullable Urn queryUrn) {
    Float finalConfidenceScore = getConfidenceScoreOrDefault(confidenceScore);
    String finalQueryUrn;
    if (queryUrn == null || StringUtils.isBlank(queryUrn.toString())) {
      finalQueryUrn = "NONE";
    } else {
      finalQueryUrn = queryUrn.toString();
    }

    ObjectNode fineGrainedLineageNode = instance.objectNode();
    fineGrainedLineageNode.put("confidenceScore", instance.numberNode(finalConfidenceScore));
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + encodeValueUrn(downstreamSchemaField)
                + "/"
                + finalQueryUrn
                + "/"
                + encodeValueUrn(upstreamSchemaField),
            fineGrainedLineageNode));

    return this;
  }

  private Float getConfidenceScoreOrDefault(@Nullable Float confidenceScore) {
    float finalConfidenceScore;
    if (confidenceScore != null && confidenceScore > 0 && confidenceScore <= 1.0f) {
      finalConfidenceScore = confidenceScore;
    } else {
      finalConfidenceScore = 1.0f;
    }

    return finalConfidenceScore;
  }

  /**
   * Removes a field as a fine grained upstream
   *
   * @param upstreamSchemaField a schema field to be marked as upstream, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param transformationOperation string operation type that describes the transformation
   *     operation happening in the lineage edge
   * @param downstreamSchemaField the downstream schema field this upstream is derived from, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param queryUrn query urn the relationship is derived from
   * @return this builder
   */
  public DataJobInputOutputPatchBuilder removeFineGrainedUpstreamField(
      @Nonnull Urn upstreamSchemaField,
      @Nonnull String transformationOperation,
      @Nonnull Urn downstreamSchemaField,
      @Nullable Urn queryUrn) {

    String finalQueryUrn;
    if (queryUrn == null || StringUtils.isBlank(queryUrn.toString())) {
      finalQueryUrn = "NONE";
    } else {
      finalQueryUrn = queryUrn.toString();
    }
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + encodeValueUrn(downstreamSchemaField)
                + "/"
                + finalQueryUrn
                + "/"
                + encodeValueUrn(upstreamSchemaField),
            null));

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
