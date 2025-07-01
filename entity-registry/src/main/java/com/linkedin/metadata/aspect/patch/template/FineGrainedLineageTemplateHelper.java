package com.linkedin.metadata.aspect.patch.template;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Streams;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.codehaus.plexus.util.StringUtils;

public class FineGrainedLineageTemplateHelper {

  private static final String FINE_GRAINED_UPSTREAM_TYPE = "upstreamType";
  private static final String FINE_GRAINED_UPSTREAMS = "upstreams";
  private static final String FINE_GRAINED_DOWNSTREAM_TYPE = "downstreamType";
  private static final String FINE_GRAINED_DOWNSTREAMS = "downstreams";
  private static final String FINE_GRAINED_TRANSFORMATION_OPERATION = "transformOperation";
  private static final String FINE_GRAINED_CONFIDENCE_SCORE = "confidenceScore";
  private static final String FINE_GRAINED_QUERY_ID = "query";

  // Template support
  private static final String NONE_TRANSFORMATION_TYPE = "NONE";
  private static final Float DEFAULT_CONFIDENCE_SCORE = 1.0f;
  private static final String DEFAULT_QUERY_ID = "NONE";

  /**
   * Combines fine grained lineage array into a map using upstream and downstream types as keys,
   * defaulting when not present. Due to this construction, patches will look like: path:
   * /fineGrainedLineages/TRANSFORMATION_OPERATION/DOWNSTREAM_FIELD_URN/QUERY_ID/UPSTREAM_FIELD_URN,
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
  public static JsonNode combineAndTransformFineGrainedLineages(
      @Nullable JsonNode fineGrainedLineages) {
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

              ArrayNode downstreams =
                  nodeClone.has(FINE_GRAINED_DOWNSTREAMS)
                      ? (ArrayNode) nodeClone.get(FINE_GRAINED_DOWNSTREAMS)
                      : null;

              if (downstreams == null || downstreams.size() != 1) {
                throw new UnsupportedOperationException(
                    "Patching not supported on fine grained lineages with not"
                        + " exactly one downstream. Current fine grained lineage implementation is downstream derived and "
                        + "patches are keyed on the root of this derivation.");
              }

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

              String queryId =
                  nodeClone.has(FINE_GRAINED_QUERY_ID)
                      ? nodeClone.get(FINE_GRAINED_QUERY_ID).asText()
                      : DEFAULT_QUERY_ID;

              if (upstreamType == null) {
                // Determine default type
                Urn upstreamUrn =
                    upstreams != null ? UrnUtils.getUrn(upstreams.get(0).asText()) : null;
                if (upstreamUrn != null
                    && DATASET_ENTITY_NAME.equals(upstreamUrn.getEntityType())) {
                  upstreamType = FINE_GRAINED_LINEAGE_DATASET_TYPE;
                } else {
                  upstreamType = FINE_GRAINED_LINEAGE_FIELD_SET_TYPE;
                }
              }

              if (downstreamType == null) {
                // Always use FIELD type, only support patches for single field downstream
                downstreamType = FINE_GRAINED_LINEAGE_FIELD_TYPE;
              }

              String downstreamRoot = downstreams.get(0).asText();
              if (!transformationOperationNode.has(downstreamRoot)) {
                transformationOperationNode.set(downstreamRoot, instance.objectNode());
              }
              ObjectNode downstreamRootNode =
                  (ObjectNode) transformationOperationNode.get(downstreamRoot);
              if (!downstreamRootNode.has(queryId)) {
                downstreamRootNode.set(queryId, instance.objectNode());
              }
              ObjectNode queryNode = (ObjectNode) downstreamRootNode.get(queryId);
              if (upstreams != null) {
                addUrnsToParent(
                    queryNode, upstreams, confidenceScore, upstreamType, downstreamType);
              }
            });
    return mapNode;
  }

  private static void addUrnsToParent(
      JsonNode parentNode,
      ArrayNode urnsList,
      Float confidenceScore,
      String upstreamType,
      String downstreamType) {
    // Will overwrite repeat urns with different confidence scores with the most recently seen
    ((ObjectNode) parentNode)
        .setAll(
            Streams.stream(urnsList.elements())
                .map(JsonNode::asText)
                .distinct()
                .collect(
                    Collectors.toMap(
                        urn -> urn,
                        urn ->
                            mapToLineageValueNode(confidenceScore, upstreamType, downstreamType))));
  }

  private static JsonNode mapToLineageValueNode(
      Float confidenceScore, String upstreamType, String downstreamType) {
    ObjectNode objectNode = instance.objectNode();
    objectNode.set(FINE_GRAINED_CONFIDENCE_SCORE, instance.numberNode(confidenceScore));
    objectNode.set(FINE_GRAINED_UPSTREAM_TYPE, instance.textNode(upstreamType));
    objectNode.set(FINE_GRAINED_DOWNSTREAM_TYPE, instance.textNode(downstreamType));
    return objectNode;
  }

  /**
   * Takes the transformed fine grained lineages map from pre-processing and reconstructs an array
   * of FineGrainedLineages Avoids producing side effects by copying nodes, use resulting node and
   * not the original
   *
   * @param transformedFineGrainedLineages the transformed fine grained lineage map
   * @return the modified {@link JsonNode} formatted consistent with the original schema
   */
  public static ArrayNode reconstructFineGrainedLineages(JsonNode transformedFineGrainedLineages) {
    if (transformedFineGrainedLineages instanceof ArrayNode) {
      // We already have an ArrayNode, no need to transform. This happens during `replace`
      // operations
      return (ArrayNode) transformedFineGrainedLineages;
    }
    ObjectNode mapNode = (ObjectNode) transformedFineGrainedLineages;
    ArrayNode fineGrainedLineages = instance.arrayNode();

    mapNode
        .fieldNames()
        .forEachRemaining(
            transformationOperation -> {
              final ObjectNode transformationOperationNode =
                  (ObjectNode) mapNode.get(transformationOperation);
              transformationOperationNode
                  .fieldNames()
                  .forEachRemaining(
                      downstreamName -> {
                        final ObjectNode downstreamNode =
                            (ObjectNode) transformationOperationNode.get(downstreamName);
                        downstreamNode
                            .fieldNames()
                            .forEachRemaining(
                                queryId ->
                                    buildFineGrainedLineage(
                                        downstreamName,
                                        downstreamNode,
                                        queryId,
                                        transformationOperation,
                                        fineGrainedLineages));
                      });
            });

    return fineGrainedLineages;
  }

  private static void buildFineGrainedLineage(
      final String downstreamName,
      final ObjectNode downstreamNode,
      final String queryId,
      final String transformationOperation,
      final ArrayNode fineGrainedLineages) {
    final ObjectNode fineGrainedLineage = instance.objectNode();
    final ObjectNode queryNode = (ObjectNode) downstreamNode.get(queryId);
    if (queryNode.isEmpty()) {
      // Short circuit if no upstreams left
      return;
    }
    ArrayNode downstream = instance.arrayNode();
    downstream.add(instance.textNode(downstreamName));
    // Set defaults, if found in sub nodes override, for confidenceScore take lowest
    AtomicReference<Float> minimumConfidenceScore = new AtomicReference<>(DEFAULT_CONFIDENCE_SCORE);
    AtomicReference<String> upstreamType =
        new AtomicReference<>(FINE_GRAINED_LINEAGE_FIELD_SET_TYPE);
    AtomicReference<String> downstreamType = new AtomicReference<>(FINE_GRAINED_LINEAGE_FIELD_TYPE);
    ArrayNode upstreams = instance.arrayNode();
    queryNode
        .fieldNames()
        .forEachRemaining(
            upstream ->
                processUpstream(
                    queryNode,
                    upstream,
                    minimumConfidenceScore,
                    upstreamType,
                    downstreamType,
                    upstreams));
    fineGrainedLineage.set(FINE_GRAINED_DOWNSTREAMS, downstream);
    fineGrainedLineage.set(FINE_GRAINED_UPSTREAMS, upstreams);
    if (StringUtils.isNotBlank(queryId) && !DEFAULT_QUERY_ID.equals(queryId)) {
      fineGrainedLineage.set(FINE_GRAINED_QUERY_ID, instance.textNode(queryId));
    }
    fineGrainedLineage.set(FINE_GRAINED_UPSTREAM_TYPE, instance.textNode(upstreamType.get()));
    fineGrainedLineage.set(FINE_GRAINED_DOWNSTREAM_TYPE, instance.textNode(downstreamType.get()));
    fineGrainedLineage.set(
        FINE_GRAINED_CONFIDENCE_SCORE, instance.numberNode(minimumConfidenceScore.get()));
    fineGrainedLineage.set(
        FINE_GRAINED_TRANSFORMATION_OPERATION, instance.textNode(transformationOperation));
    fineGrainedLineages.add(fineGrainedLineage);
  }

  private static void processUpstream(
      final ObjectNode queryNode,
      final String upstream,
      final AtomicReference<Float> minimumConfidenceScore,
      final AtomicReference<String> upstreamType,
      final AtomicReference<String> downstreamType,
      final ArrayNode upstreams) {
    final ObjectNode upstreamNode = (ObjectNode) queryNode.get(upstream);
    if (upstreamNode.has(FINE_GRAINED_CONFIDENCE_SCORE)) {
      Float scoreValue = upstreamNode.get(FINE_GRAINED_CONFIDENCE_SCORE).floatValue();
      if (scoreValue <= minimumConfidenceScore.get()) {
        minimumConfidenceScore.set(scoreValue);
      }
    }
    // Set types to last encountered, should never change, but this at least tries to support
    // other types being specified.
    if (upstreamNode.has(FINE_GRAINED_UPSTREAM_TYPE)) {
      upstreamType.set(upstreamNode.get(FINE_GRAINED_UPSTREAM_TYPE).asText());
    }
    if (upstreamNode.has(FINE_GRAINED_DOWNSTREAM_TYPE)) {
      downstreamType.set(upstreamNode.get(FINE_GRAINED_DOWNSTREAM_TYPE).asText());
    }
    upstreams.add(instance.textNode(upstream));
  }
}
