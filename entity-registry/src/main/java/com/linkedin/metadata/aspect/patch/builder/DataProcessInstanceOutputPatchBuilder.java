// ABOUTME: Patch builder for DataProcessInstanceOutput aspect.
// ABOUTME: Supports adding/removing outputs (URN array) and outputEdges (Edge array).
package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME;
import static com.linkedin.metadata.aspect.patch.builder.PatchUtil.createEdgeValue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DataProcessInstanceOutputPatchBuilder
    extends AbstractMultiFieldPatchBuilder<DataProcessInstanceOutputPatchBuilder> {

  private static final String OUTPUTS_PATH_START = "/outputs/";
  private static final String OUTPUT_EDGES_PATH_START = "/outputEdges/";

  /**
   * Adds an output URN to the outputs array.
   *
   * @param urn the output URN to add
   * @return this builder
   */
  public DataProcessInstanceOutputPatchBuilder addOutput(@Nonnull Urn urn) {
    TextNode textNode = instance.textNode(urn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            OUTPUTS_PATH_START + encodeValue(urn.toString()),
            textNode));
    return this;
  }

  /**
   * Removes an output URN from the outputs array.
   *
   * @param urn the output URN to remove
   * @return this builder
   */
  public DataProcessInstanceOutputPatchBuilder removeOutput(@Nonnull Urn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            OUTPUTS_PATH_START + encodeValue(urn.toString()),
            null));
    return this;
  }

  /**
   * Adds an output edge with the given destination URN.
   *
   * @param destinationUrn the destination URN of the edge
   * @return this builder
   */
  public DataProcessInstanceOutputPatchBuilder addOutputEdge(@Nonnull Urn destinationUrn) {
    ObjectNode value = createEdgeValue(destinationUrn);
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            OUTPUT_EDGES_PATH_START + encodeValue(destinationUrn.toString()),
            value));
    return this;
  }

  /**
   * Adds an output edge with full Edge properties.
   *
   * @param edge the edge to add
   * @return this builder
   */
  public DataProcessInstanceOutputPatchBuilder addOutputEdge(@Nonnull Edge edge) {
    ObjectNode value = createEdgeValue(edge);
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            OUTPUT_EDGES_PATH_START + encodeValue(edge.getDestinationUrn().toString()),
            value));
    return this;
  }

  /**
   * Removes an output edge by its destination URN.
   *
   * @param destinationUrn the destination URN of the edge to remove
   * @return this builder
   */
  public DataProcessInstanceOutputPatchBuilder removeOutputEdge(@Nonnull Urn destinationUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            OUTPUT_EDGES_PATH_START + encodeValue(destinationUrn.toString()),
            null));
    return this;
  }

  @Override
  protected String getAspectName() {
    return DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATA_PROCESS_INSTANCE_ENTITY_NAME;
  }
}
