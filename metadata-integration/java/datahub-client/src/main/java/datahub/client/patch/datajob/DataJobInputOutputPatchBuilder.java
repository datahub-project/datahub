package datahub.client.patch.datajob;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


public class DataJobInputOutputPatchBuilder extends AbstractMultiFieldPatchBuilder<DataJobInputOutputPatchBuilder> {
  private static final String INPUT_DATA_JOB_EDGES_PATH_START = "/inputDatajobEdges/";
  private static final String INPUT_DATASET_EDGES_PATH_START = "/inputDatasetEdges/";
  private static final String OUTPUT_DATASET_EDGES_PATH_START = "/outputDatasetEdges/";

  private static final String DESTINATION_URN_KEY = "destinationUrn";
  private static final String LAST_MODIFIED_KEY = "lastModified";
  private static final String CREATED_KEY = "created";

  private static final String INPUT_DATASET_FIELDS_PATH_START = "/inputDatasetFields/";
  private static final String OUTPUT_DATASET_FIELDS_PATH_START = "/outputDatasetFields/";

  private static final String TIME_KEY = "time";
  private static final String ACTOR_KEY = "actor";

  public DataJobInputOutputPatchBuilder addInputDatajobEdge(@Nonnull DataJobUrn dataJobUrn) {
    ObjectNode value = createEdgeValue(dataJobUrn);

    pathValues.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), INPUT_DATA_JOB_EDGES_PATH_START + dataJobUrn, value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeInputDatajobEdge(@Nonnull DataJobUrn dataJobUrn) {
    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), INPUT_DATA_JOB_EDGES_PATH_START + dataJobUrn, null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addInputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    ObjectNode value = createEdgeValue(datasetUrn);

    pathValues.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), INPUT_DATASET_EDGES_PATH_START + datasetUrn, value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeInputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), INPUT_DATASET_EDGES_PATH_START + datasetUrn, null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addOutputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    ObjectNode value = createEdgeValue(datasetUrn);

    pathValues.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), OUTPUT_DATASET_EDGES_PATH_START + datasetUrn, value));
    return this;
  }

  public DataJobInputOutputPatchBuilder removeOutputDatasetEdge(@Nonnull DatasetUrn datasetUrn) {
    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), OUTPUT_DATASET_EDGES_PATH_START + datasetUrn, null));
    return this;
  }

  private ObjectNode createEdgeValue(Urn urn) {
    ObjectNode value = instance.objectNode();
    ObjectNode auditStamp = instance.objectNode();
    auditStamp.put(TIME_KEY, System.currentTimeMillis())
        .put(ACTOR_KEY, UNKNOWN_ACTOR);

    value.put(DESTINATION_URN_KEY, urn.toString())
        .set(LAST_MODIFIED_KEY, auditStamp);
    value.set(CREATED_KEY, auditStamp);

    return value;
  }

  public DataJobInputOutputPatchBuilder addInputDatasetField(@Nonnull Urn urn) {
    TextNode textNode = instance.textNode(urn.toString());
    pathValues.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), INPUT_DATASET_FIELDS_PATH_START + urn, textNode));

    return this;
  }

  public DataJobInputOutputPatchBuilder removeInputDatasetField(@Nonnull Urn urn) {
    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), INPUT_DATASET_FIELDS_PATH_START + urn, null));
    return this;
  }

  public DataJobInputOutputPatchBuilder addOutputDatasetField(@Nonnull Urn urn) {
    TextNode textNode = instance.textNode(urn.toString());
    pathValues.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), OUTPUT_DATASET_FIELDS_PATH_START + urn, textNode));

    return this;
  }

  public DataJobInputOutputPatchBuilder removeOutputDatasetField(@Nonnull Urn urn) {
    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), OUTPUT_DATASET_FIELDS_PATH_START + urn, null));
    return this;
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
