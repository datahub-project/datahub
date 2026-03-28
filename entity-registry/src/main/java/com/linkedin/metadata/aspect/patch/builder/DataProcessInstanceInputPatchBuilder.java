// ABOUTME: Patch builder for DataProcessInstanceInput aspect.
// ABOUTME: Supports adding/removing inputs (URN array) and inputEdges (Edge array).
package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.aspect.patch.builder.PatchUtil.createEdgeValue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DataProcessInstanceInputPatchBuilder
    extends AbstractMultiFieldPatchBuilder<DataProcessInstanceInputPatchBuilder> {

  private static final String INPUTS_PATH_START = "/inputs/";
  private static final String INPUT_EDGES_PATH_START = "/inputEdges/";

  /**
   * Adds an input URN to the inputs array.
   *
   * @param urn the input URN to add
   * @return this builder
   */
  public DataProcessInstanceInputPatchBuilder addInput(@Nonnull Urn urn) {
    TextNode textNode = instance.textNode(urn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            INPUTS_PATH_START + encodeValue(urn.toString()),
            textNode));
    return this;
  }

  /**
   * Removes an input URN from the inputs array.
   *
   * @param urn the input URN to remove
   * @return this builder
   */
  public DataProcessInstanceInputPatchBuilder removeInput(@Nonnull Urn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            INPUTS_PATH_START + encodeValue(urn.toString()),
            null));
    return this;
  }

  /**
   * Adds an input edge with the given destination URN.
   *
   * @param destinationUrn the destination URN of the edge
   * @return this builder
   */
  public DataProcessInstanceInputPatchBuilder addInputEdge(@Nonnull Urn destinationUrn) {
    ObjectNode value = createEdgeValue(destinationUrn);
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            INPUT_EDGES_PATH_START + encodeValue(destinationUrn.toString()),
            value));
    return this;
  }

  /**
   * Adds an input edge with full Edge properties.
   *
   * @param edge the edge to add
   * @return this builder
   */
  public DataProcessInstanceInputPatchBuilder addInputEdge(@Nonnull Edge edge) {
    ObjectNode value = createEdgeValue(edge);
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            INPUT_EDGES_PATH_START + encodeValue(edge.getDestinationUrn().toString()),
            value));
    return this;
  }

  /**
   * Removes an input edge by its destination URN.
   *
   * @param destinationUrn the destination URN of the edge to remove
   * @return this builder
   */
  public DataProcessInstanceInputPatchBuilder removeInputEdge(@Nonnull Urn destinationUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            INPUT_EDGES_PATH_START + encodeValue(destinationUrn.toString()),
            null));
    return this;
  }

  @Override
  protected String getAspectName() {
    return DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATA_PROCESS_INSTANCE_ENTITY_NAME;
  }
}
