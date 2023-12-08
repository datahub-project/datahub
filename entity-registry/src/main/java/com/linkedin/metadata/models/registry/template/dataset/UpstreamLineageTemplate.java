package com.linkedin.metadata.models.registry.template.dataset;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Streams;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.models.registry.template.CompoundKeyTemplate;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UpstreamLineageTemplate extends CompoundKeyTemplate<UpstreamLineage> {

  // Fields
  private static final String UPSTREAMS_FIELD_NAME = "upstreams";
  private static final String DATASET_FIELD_NAME = "dataset";
  private static final String FINE_GRAINED_LINEAGES_FIELD_NAME = "fineGrainedLineages";
  private static final String FINE_GRAINED_UPSTREAM_TYPE = "upstreamType";
  private static final String FINE_GRAINED_UPSTREAMS = "upstreams";
  private static final String FINE_GRAINED_DOWNSTREAM_TYPE = "downstreamType";
  private static final String FINE_GRAINED_DOWNSTREAMS = "downstreams";
  private static final String FINE_GRAINED_TRANSFORMATION_OPERATION = "transformOperation";
  private static final String FINE_GRAINED_CONFIDENCE_SCORE = "confidenceScore";

  // Template support
  private static final String NONE_TRANSFORMATION_TYPE = "NONE";
  private static final Float DEFAULT_CONFIDENCE_SCORE = 1.0f;

  @Override
  public UpstreamLineage getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof UpstreamLineage) {
      return (UpstreamLineage) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to UpstreamLineage");
  }

  @Override
  public Class<UpstreamLineage> getTemplateType() {
    return UpstreamLineage.class;
  }

  @Nonnull
  @Override
  public UpstreamLineage getDefault() {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    upstreamLineage.setUpstreams(new UpstreamArray());
    upstreamLineage.setFineGrainedLineages(new FineGrainedLineageArray());

    return upstreamLineage;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode, UPSTREAMS_FIELD_NAME, Collections.singletonList(DATASET_FIELD_NAME));
    ((ObjectNode) transformedNode)
        .set(
            FINE_GRAINED_LINEAGES_FIELD_NAME,
            combineAndTransformFineGrainedLineages(
                transformedNode.get(FINE_GRAINED_LINEAGES_FIELD_NAME)));

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched, UPSTREAMS_FIELD_NAME, Collections.singletonList(DATASET_FIELD_NAME));
    ((ObjectNode) rebasedNode)
        .set(
            FINE_GRAINED_LINEAGES_FIELD_NAME,
            reconstructFineGrainedLineages(rebasedNode.get(FINE_GRAINED_LINEAGES_FIELD_NAME)));
    return rebasedNode;
  }

  /**
   * Combines fine grained lineage array into a map using upstream and downstream types as keys,
   * defaulting when not present. Due to this construction, patches will look like: path:
   * /fineGrainedLineages/TRANSFORMATION_OPERATION/(upstreamType || downstreamType)/TYPE/FIELD_URN,
   * op: ADD/REMOVE, value: float (confidenceScore) Due to the way FineGrainedLineage was designed
   * it doesn't necessarily have a consistent key we can reference, so this specialized method
   * mimics the arrayFieldToMap of the super class with the specialization that it does not put the
   * full value of the aspect at the end of the key, just the particular array. This prevents
   * unintended overwrites through improper MCP construction that is technically allowed by the
   * schema when combining under fields that form the natural key.
   *
   * @param fineGrainedLineages the fine grained lineage array node
   * @return the modified {@link JsonNode} with array fields transformed to maps
   */
  private JsonNode combineAndTransformFineGrainedLineages(@Nullable JsonNode fineGrainedLineages) {
    ObjectNode mapNode = instance.objectNode();
    if (!(fineGrainedLineages instanceof ArrayNode) || fineGrainedLineages.isEmpty()) {
      return mapNode;
    }
    JsonNode lineageCopy = fineGrainedLineages.deepCopy();

    lineageCopy
        .elements()
        .forEachRemaining(
            node -> {
              JsonNode nodeClone = node.deepCopy();
              String transformationOperation =
                  nodeClone.has(FINE_GRAINED_TRANSFORMATION_OPERATION)
                      ? nodeClone.get(FINE_GRAINED_TRANSFORMATION_OPERATION).asText()
                      : NONE_TRANSFORMATION_TYPE;

              if (!mapNode.has(transformationOperation)) {
                mapNode.set(transformationOperation, instance.objectNode());
              }
              ObjectNode transformationOperationNode =
                  (ObjectNode) mapNode.get(transformationOperation);

              Float confidenceScore =
                  nodeClone.has(FINE_GRAINED_CONFIDENCE_SCORE)
                      ? nodeClone.get(FINE_GRAINED_CONFIDENCE_SCORE).floatValue()
                      : DEFAULT_CONFIDENCE_SCORE;

              String upstreamType =
                  nodeClone.has(FINE_GRAINED_UPSTREAM_TYPE)
                      ? nodeClone.get(FINE_GRAINED_UPSTREAM_TYPE).asText()
                      : null;
              String downstreamType =
                  nodeClone.has(FINE_GRAINED_DOWNSTREAM_TYPE)
                      ? nodeClone.get(FINE_GRAINED_DOWNSTREAM_TYPE).asText()
                      : null;
              ArrayNode upstreams =
                  nodeClone.has(FINE_GRAINED_UPSTREAMS)
                      ? (ArrayNode) nodeClone.get(FINE_GRAINED_UPSTREAMS)
                      : null;
              ArrayNode downstreams =
                  nodeClone.has(FINE_GRAINED_DOWNSTREAMS)
                      ? (ArrayNode) nodeClone.get(FINE_GRAINED_DOWNSTREAMS)
                      : null;

              // Handle upstreams
              if (upstreamType == null) {
                // Determine default type
                Urn upstreamUrn =
                    upstreams != null ? UrnUtils.getUrn(upstreams.get(0).asText()) : null;
                if (upstreamUrn != null
                    && SCHEMA_FIELD_ENTITY_NAME.equals(upstreamUrn.getEntityType())) {
                  upstreamType = FINE_GRAINED_LINEAGE_FIELD_SET_TYPE;
                } else {
                  upstreamType = FINE_GRAINED_LINEAGE_DATASET_TYPE;
                }
              }
              if (!transformationOperationNode.has(FINE_GRAINED_UPSTREAM_TYPE)) {
                transformationOperationNode.set(FINE_GRAINED_UPSTREAM_TYPE, instance.objectNode());
              }
              ObjectNode upstreamTypeNode =
                  (ObjectNode) transformationOperationNode.get(FINE_GRAINED_UPSTREAM_TYPE);
              if (!upstreamTypeNode.has(upstreamType)) {
                upstreamTypeNode.set(upstreamType, instance.objectNode());
              }
              if (upstreams != null) {
                addUrnsToSubType(upstreamTypeNode, upstreams, upstreamType, confidenceScore);
              }

              // Handle downstreams
              if (downstreamType == null) {
                // Determine default type
                if (downstreams != null && downstreams.size() > 1) {
                  downstreamType = FINE_GRAINED_LINEAGE_FIELD_SET_TYPE;
                } else {
                  downstreamType = FINE_GRAINED_LINEAGE_FIELD_TYPE;
                }
              }
              if (!transformationOperationNode.has(FINE_GRAINED_DOWNSTREAM_TYPE)) {
                transformationOperationNode.set(
                    FINE_GRAINED_DOWNSTREAM_TYPE, instance.objectNode());
              }
              ObjectNode downstreamTypeNode =
                  (ObjectNode) transformationOperationNode.get(FINE_GRAINED_DOWNSTREAM_TYPE);
              if (!downstreamTypeNode.has(downstreamType)) {
                downstreamTypeNode.set(downstreamType, instance.objectNode());
              }
              if (downstreams != null) {
                addUrnsToSubType(downstreamTypeNode, downstreams, downstreamType, confidenceScore);
              }
            });
    return mapNode;
  }

  private void addUrnsToSubType(
      JsonNode superType, ArrayNode urnsList, String subType, Float confidenceScore) {
    ObjectNode upstreamSubTypeNode = (ObjectNode) superType.get(subType);
    // Will overwrite repeat urns with different confidence scores with the most recently seen
    upstreamSubTypeNode.setAll(
        Streams.stream(urnsList.elements())
            .map(JsonNode::asText)
            .distinct()
            .collect(Collectors.toMap(urn -> urn, urn -> instance.numberNode(confidenceScore))));
  }

  /**
   * Takes the transformed fine grained lineages map from pre-processing and reconstructs an array
   * of FineGrainedLineages Avoids producing side effects by copying nodes, use resulting node and
   * not the original
   *
   * @param transformedFineGrainedLineages the transformed fine grained lineage map
   * @return the modified {@link JsonNode} formatted consistent with the original schema
   */
  private ArrayNode reconstructFineGrainedLineages(JsonNode transformedFineGrainedLineages) {
    if (transformedFineGrainedLineages instanceof ArrayNode) {
      // We already have an ArrayNode, no need to transform. This happens during `replace`
      // operations
      return (ArrayNode) transformedFineGrainedLineages;
    }
    ObjectNode mapNode = (ObjectNode) transformedFineGrainedLineages;
    ArrayNode arrayNode = instance.arrayNode();

    mapNode
        .fieldNames()
        .forEachRemaining(
            transformationOperation -> {
              final ObjectNode transformationOperationNode =
                  (ObjectNode) mapNode.get(transformationOperation);
              final ObjectNode upstreamType =
                  transformationOperationNode.has(FINE_GRAINED_UPSTREAM_TYPE)
                      ? (ObjectNode) transformationOperationNode.get(FINE_GRAINED_UPSTREAM_TYPE)
                      : instance.objectNode();
              final ObjectNode downstreamType =
                  transformationOperationNode.has(FINE_GRAINED_DOWNSTREAM_TYPE)
                      ? (ObjectNode) transformationOperationNode.get(FINE_GRAINED_DOWNSTREAM_TYPE)
                      : instance.objectNode();

              // Handle upstreams
              if (!upstreamType.isEmpty()) {
                populateTypeNode(
                    upstreamType,
                    transformationOperation,
                    FINE_GRAINED_UPSTREAM_TYPE,
                    FINE_GRAINED_UPSTREAMS,
                    FINE_GRAINED_DOWNSTREAM_TYPE,
                    arrayNode);
              }

              // Handle downstreams
              if (!downstreamType.isEmpty()) {
                populateTypeNode(
                    downstreamType,
                    transformationOperation,
                    FINE_GRAINED_DOWNSTREAM_TYPE,
                    FINE_GRAINED_DOWNSTREAMS,
                    FINE_GRAINED_UPSTREAM_TYPE,
                    arrayNode);
              }
            });

    return arrayNode;
  }

  private void populateTypeNode(
      JsonNode typeNode,
      String transformationOperation,
      String typeName,
      String arrayTypeName,
      String defaultTypeName,
      ArrayNode arrayNode) {
    typeNode
        .fieldNames()
        .forEachRemaining(
            subTypeName -> {
              ObjectNode subType = (ObjectNode) typeNode.get(subTypeName);
              if (!subType.isEmpty()) {
                ObjectNode fineGrainedLineage = instance.objectNode();
                AtomicReference<Float> minimumConfidenceScore = new AtomicReference<>(1.0f);

                fineGrainedLineage.put(typeName, subTypeName);
                fineGrainedLineage.put(
                    FINE_GRAINED_TRANSFORMATION_OPERATION, transformationOperation);
                // Array to actually be filled out
                fineGrainedLineage.set(arrayTypeName, instance.arrayNode());
                // Added to pass model validation, because we have no way of appropriately pairing
                // upstreams and downstreams
                // within fine grained lineages consistently due to being able to have multiple
                // downstream types paired with a single
                // transform operation, we just set a default type because it's a required property
                fineGrainedLineage.put(defaultTypeName, FINE_GRAINED_LINEAGE_FIELD_SET_TYPE);
                subType
                    .fieldNames()
                    .forEachRemaining(
                        subTypeKey -> {
                          ((ArrayNode) fineGrainedLineage.get(arrayTypeName)).add(subTypeKey);
                          Float scoreValue = subType.get(subTypeKey).floatValue();
                          if (scoreValue <= minimumConfidenceScore.get()) {
                            minimumConfidenceScore.set(scoreValue);
                            fineGrainedLineage.set(
                                FINE_GRAINED_CONFIDENCE_SCORE,
                                instance.numberNode(minimumConfidenceScore.get()));
                          }
                        });
                arrayNode.add(fineGrainedLineage);
              }
            });
  }
}
