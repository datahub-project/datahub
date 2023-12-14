package datahub.client.patch.datajob;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.LineageDirection;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DataJobInputOutputPatchBuilder
    extends AbstractMultiFieldPatchBuilder<DataJobInputOutputPatchBuilder> {
  private static final String INPUT_DATA_JOB_EDGES_PATH_START = "/inputDatajobEdges/";
  private static final String INPUT_DATASET_EDGES_PATH_START = "/inputDatasetEdges/";
  private static final String OUTPUT_DATASET_EDGES_PATH_START = "/outputDatasetEdges/";

  private static final String DESTINATION_URN_KEY = "destinationUrn";
  private static final String SOURCE_URN_KEY = "sourceUrn";
  private static final String LAST_MODIFIED_KEY = "lastModified";
  private static final String CREATED_KEY = "created";
  private static final String PROPERTIES_KEY = "properties";

  private static final String INPUT_DATASET_FIELDS_PATH_START = "/inputDatasetFields/";
  private static final String OUTPUT_DATASET_FIELDS_PATH_START = "/outputDatasetFields/";

  private static final String TIME_KEY = "time";
  private static final String ACTOR_KEY = "actor";
  private static final String IMPERSONATOR_KEY = "impersonator";
  private static final String MESSAGE_KEY = "message";

  // Simplified with just Urn
  public DataJobInputOutputPatchBuilder addInputDatajobEdge(@Nonnull DataJobUrn dataJobUrn) {
    ObjectNode value = createEdgeValue(dataJobUrn);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            INPUT_DATA_JOB_EDGES_PATH_START + dataJobUrn,
            value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeInputDatajobEdge(@Nonnull DataJobUrn dataJobUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            INPUT_DATA_JOB_EDGES_PATH_START + dataJobUrn,
            null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addInputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    ObjectNode value = createEdgeValue(datasetUrn);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), INPUT_DATASET_EDGES_PATH_START + datasetUrn, value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeInputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            INPUT_DATASET_EDGES_PATH_START + datasetUrn,
            null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addOutputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    ObjectNode value = createEdgeValue(datasetUrn);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            OUTPUT_DATASET_EDGES_PATH_START + datasetUrn,
            value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeOutputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            OUTPUT_DATASET_EDGES_PATH_START + datasetUrn,
            null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addInputDatasetField(@Nonnull Urn urn) {
    TextNode textNode = instance.textNode(urn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), INPUT_DATASET_FIELDS_PATH_START + urn, textNode));

    return this;
  }

  public DataJobInputOutputPatchBuilder removeInputDatasetField(@Nonnull Urn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), INPUT_DATASET_FIELDS_PATH_START + urn, null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addOutputDatasetField(@Nonnull Urn urn) {
    TextNode textNode = instance.textNode(urn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), OUTPUT_DATASET_FIELDS_PATH_START + urn, textNode));

    return this;
  }

  public DataJobInputOutputPatchBuilder removeOutputDatasetField(@Nonnull Urn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), OUTPUT_DATASET_FIELDS_PATH_START + urn, null));
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

  private ObjectNode createEdgeValue(@Nonnull Urn urn) {
    ObjectNode value = instance.objectNode();
    ObjectNode auditStamp = instance.objectNode();
    auditStamp.put(TIME_KEY, System.currentTimeMillis()).put(ACTOR_KEY, UNKNOWN_ACTOR);

    value.put(DESTINATION_URN_KEY, urn.toString()).set(LAST_MODIFIED_KEY, auditStamp);
    value.set(CREATED_KEY, auditStamp);

    return value;
  }

  private ObjectNode createEdgeValue(@Nonnull Edge edge) {
    ObjectNode value = instance.objectNode();

    ObjectNode created = instance.objectNode();
    if (edge.getCreated() == null) {
      created.put(TIME_KEY, System.currentTimeMillis()).put(ACTOR_KEY, UNKNOWN_ACTOR);
    } else {
      created
          .put(TIME_KEY, edge.getCreated().getTime())
          .put(ACTOR_KEY, edge.getCreated().getActor().toString());
      if (edge.getCreated().getImpersonator() != null) {
        created.put(IMPERSONATOR_KEY, edge.getCreated().getImpersonator().toString());
      }
      if (edge.getCreated().getMessage() != null) {
        created.put(MESSAGE_KEY, edge.getCreated().getMessage());
      }
    }
    value.set(CREATED_KEY, created);

    ObjectNode lastModified = instance.objectNode();
    if (edge.getLastModified() == null) {
      lastModified.put(TIME_KEY, System.currentTimeMillis()).put(ACTOR_KEY, UNKNOWN_ACTOR);
    } else {
      lastModified
          .put(TIME_KEY, edge.getLastModified().getTime())
          .put(ACTOR_KEY, edge.getLastModified().getActor().toString());
      if (edge.getLastModified().getImpersonator() != null) {
        lastModified.put(IMPERSONATOR_KEY, edge.getLastModified().getImpersonator().toString());
      }
      if (edge.getLastModified().getMessage() != null) {
        lastModified.put(MESSAGE_KEY, edge.getLastModified().getMessage());
      }
    }
    value.set(LAST_MODIFIED_KEY, lastModified);

    if (edge.getProperties() != null) {
      ObjectNode propertiesNode = instance.objectNode();
      edge.getProperties().forEach((k, v) -> propertiesNode.set(k, instance.textNode(v)));
      value.set(PROPERTIES_KEY, propertiesNode);
    }

    value.put(DESTINATION_URN_KEY, edge.getDestinationUrn().toString());
    if (edge.getSourceUrn() != null) {
      value.put(SOURCE_URN_KEY, edge.getSourceUrn().toString());
    }

    return value;
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
      return INPUT_DATASET_EDGES_PATH_START + destinationUrn;
    }

    if (DATASET_ENTITY_NAME.equals(destinationUrn.getEntityType())
        && LineageDirection.DOWNSTREAM.equals(direction)) {
      return INPUT_DATASET_EDGES_PATH_START + destinationUrn;
    }

    if (DATA_JOB_ENTITY_NAME.equals(destinationUrn.getEntityType())
        && LineageDirection.UPSTREAM.equals(direction)) {
      return INPUT_DATA_JOB_EDGES_PATH_START + destinationUrn;
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
