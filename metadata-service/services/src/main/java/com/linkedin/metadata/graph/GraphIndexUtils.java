package com.linkedin.metadata.graph;

import com.datahub.util.RecordUtils;
import com.linkedin.common.InputField;
import com.linkedin.common.InputFields;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphIndexUtils {
  private static final String DOWNSTREAM_OF = "DownstreamOf";

  private GraphIndexUtils() {}

  @Nullable
  private static <T> List<T> getList(
      @Nullable final String path, @Nonnull final RecordTemplate aspect) {
    if (path == null) {
      return null;
    }
    final PathSpec pathSpec = new PathSpec(path.split("/"));
    final Object value = RecordUtils.getNullableFieldValue(aspect, pathSpec);
    if (FieldExtractor.getNumArrayWildcards(pathSpec) > 0) {
      return (List<T>) value;
    } else {
      return value != null ? List.of((T) value) : null;
    }
  }

  private static boolean isValueListValid(
      @Nullable final List<?> entryList, final int valueListSize) {
    if (entryList == null) {
      return false;
    }
    return valueListSize == entryList.size();
  }

  @Nullable
  private static <T> T getValue(
      @Nullable final List<T> list, final int index, final int valueListSize) {
    if (isValueListValid(list, valueListSize)) {
      return list.get(index);
    }
    return null;
  }

  /**
   * Used to create new edges for the graph db, adding all the metadata associated with each edge
   * based on the aspect. Returns a list of Edges to be consumed by the graph service.
   */
  @Nonnull
  public static List<Edge> extractGraphEdges(
      @Nonnull final Map.Entry<RelationshipFieldSpec, List<Object>> extractedFieldsEntry,
      @Nonnull final RecordTemplate aspect,
      @Nonnull final Urn urn,
      @Nonnull final MetadataChangeLog event,
      @Nonnull final boolean isNewAspectVersion) {
    final List<Edge> edgesToAdd = new ArrayList<>();
    final String createdOnPath =
        extractedFieldsEntry.getKey().getRelationshipAnnotation().getCreatedOn();
    final String createdActorPath =
        extractedFieldsEntry.getKey().getRelationshipAnnotation().getCreatedActor();
    final String updatedOnPath =
        extractedFieldsEntry.getKey().getRelationshipAnnotation().getUpdatedOn();
    final String updatedActorPath =
        extractedFieldsEntry.getKey().getRelationshipAnnotation().getUpdatedActor();
    final String propertiesPath =
        extractedFieldsEntry.getKey().getRelationshipAnnotation().getProperties();
    final String viaNodePath = extractedFieldsEntry.getKey().getRelationshipAnnotation().getVia();

    final List<Long> createdOnList = getList(createdOnPath, aspect);
    final List<Urn> createdActorList = getList(createdActorPath, aspect);
    final List<Long> updatedOnList = getList(updatedOnPath, aspect);
    final List<Urn> updatedActorList = getList(updatedActorPath, aspect);
    final List<Map<String, Object>> propertiesList = getList(propertiesPath, aspect);
    final List<Urn> viaList = getList(viaNodePath, aspect);

    int index = 0;
    for (Object fieldValue : extractedFieldsEntry.getValue()) {
      Long createdOn =
          createdOnList != null
              ? getValue(createdOnList, index, extractedFieldsEntry.getValue().size())
              : null;
      Urn createdActor =
          createdActorList != null
              ? getValue(createdActorList, index, extractedFieldsEntry.getValue().size())
              : null;
      Long updatedOn =
          updatedOnList != null
              ? getValue(updatedOnList, index, extractedFieldsEntry.getValue().size())
              : null;
      Urn updatedActor =
          updatedActorList != null
              ? getValue(updatedActorList, index, extractedFieldsEntry.getValue().size())
              : null;
      final Map<String, Object> properties =
          propertiesList != null
              ? getValue(propertiesList, index, extractedFieldsEntry.getValue().size())
              : null;

      Urn viaNode =
          viaNodePath != null
              ? getValue(viaList, index, extractedFieldsEntry.getValue().size())
              : null;

      SystemMetadata systemMetadata;
      if (isNewAspectVersion) {
        systemMetadata = event.hasSystemMetadata() ? event.getSystemMetadata() : null;
      } else {
        systemMetadata =
            event.hasPreviousSystemMetadata() ? event.getPreviousSystemMetadata() : null;
      }

      if ((createdOn == null || createdOn == 0) && systemMetadata != null) {
        createdOn = systemMetadata.getLastObserved();
      }

      if ((updatedOn == null || updatedOn == 0) && systemMetadata != null) {
        updatedOn = systemMetadata.getLastObserved();
      }

      if (createdActor == null && event.hasCreated()) {
        createdActor = event.getCreated().getActor();
        updatedActor = event.getCreated().getActor();
      }

      try {
        edgesToAdd.add(
            new Edge(
                urn,
                Urn.createFromString(fieldValue.toString()),
                extractedFieldsEntry.getKey().getRelationshipName(),
                createdOn,
                createdActor,
                updatedOn,
                updatedActor,
                properties,
                null,
                viaNode));
      } catch (URISyntaxException e) {
        log.error("Invalid destination urn: {}", fieldValue, e);
      }
      index++;
    }
    return edgesToAdd;
  }

  @Nonnull
  public static Edge mergeEdges(@Nonnull final Edge oldEdge, @Nonnull final Edge newEdge) {
    // Set createdOn and createdActor to null, so they don't get overwritten in the Graph Index.
    // A merged edge is really just an edge that's getting updated.
    return new Edge(
        oldEdge.getSource(),
        oldEdge.getDestination(),
        oldEdge.getRelationshipType(),
        null,
        null,
        newEdge.getUpdatedOn(),
        newEdge.getUpdatedActor(),
        newEdge.getProperties(),
        oldEdge.getLifecycleOwner(),
        oldEdge.getVia(),
        oldEdge.getViaStatus(),
        oldEdge.getLifecycleOwnerStatus(),
        oldEdge.getSourceStatus(),
        oldEdge.getDestinationStatus());
  }

  // TODO: remove this method once we implement sourceOverride when creating graph edges
  private static void updateFineGrainedEdgesAndRelationships(
      Urn entity,
      FineGrainedLineageArray fineGrainedLineageArray,
      List<Edge> edgesToAdd,
      HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded) {
    if (fineGrainedLineageArray != null) {
      for (FineGrainedLineage fineGrainedLineage : fineGrainedLineageArray) {
        if (!fineGrainedLineage.hasDownstreams() || !fineGrainedLineage.hasUpstreams()) {
          break;
        }
        // Fine grained lineage array is present either on datajob (datajob input/output) or dataset
        // We set the datajob as the viaEntity in scenario 1, and the query (if present) as the
        // viaEntity in scenario 2
        Urn viaEntity =
            entity.getEntityType().equals("dataJob") ? entity : fineGrainedLineage.getQuery();
        // for every downstream, create an edge with each of the upstreams
        for (Urn downstream : fineGrainedLineage.getDownstreams()) {
          for (Urn upstream : fineGrainedLineage.getUpstreams()) {
            // TODO: add edges uniformly across aspects
            edgesToAdd.add(
                new Edge(
                    downstream,
                    upstream,
                    DOWNSTREAM_OF,
                    null,
                    null,
                    null,
                    null,
                    null,
                    entity,
                    viaEntity));
            Set<String> relationshipTypes =
                urnToRelationshipTypesBeingAdded.getOrDefault(downstream, new HashSet<>());
            relationshipTypes.add(DOWNSTREAM_OF);
            urnToRelationshipTypesBeingAdded.put(downstream, relationshipTypes);
          }
        }
      }
    }
  }

  // TODO: remove this method once we implement sourceOverride and update inputFields aspect
  private static void updateInputFieldEdgesAndRelationships(
      @Nonnull final Urn urn,
      @Nonnull final InputFields inputFields,
      @Nonnull final List<Edge> edgesToAdd,
      @Nonnull final HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded) {
    if (inputFields.hasFields()) {
      for (final InputField field : inputFields.getFields()) {
        if (field.hasSchemaFieldUrn()
            && field.hasSchemaField()
            && field.getSchemaField().hasFieldPath()) {
          final Urn sourceFieldUrn =
              SchemaFieldUtils.generateSchemaFieldUrn(urn, field.getSchemaField().getFieldPath());
          // TODO: add edges uniformly across aspects
          edgesToAdd.add(
              new Edge(
                  sourceFieldUrn,
                  field.getSchemaFieldUrn(),
                  DOWNSTREAM_OF,
                  null,
                  null,
                  null,
                  null,
                  null));
          final Set<String> relationshipTypes =
              urnToRelationshipTypesBeingAdded.getOrDefault(sourceFieldUrn, new HashSet<>());
          relationshipTypes.add(DOWNSTREAM_OF);
          urnToRelationshipTypesBeingAdded.put(sourceFieldUrn, relationshipTypes);
        }
      }
    }
  }

  public static Pair<List<Edge>, HashMap<Urn, Set<String>>> getEdgesAndRelationshipTypesFromAspect(
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final RecordTemplate aspect,
      @Nonnull final MetadataChangeLog event,
      final boolean isNewAspectVersion) {
    final List<Edge> edges = new ArrayList<>();
    final HashMap<Urn, Set<String>> urnToRelationshipTypes = new HashMap<>();

    // we need to manually set schemaField <-> schemaField edges for fineGrainedLineage and
    // inputFields
    // since @Relationship only links between the parent entity urn and something else.
    if (aspectSpec.getName() == null) {
      return Pair.of(edges, urnToRelationshipTypes);
    }

    if (aspectSpec.getName().equals(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)) {
      UpstreamLineage upstreamLineage = new UpstreamLineage(aspect.data());
      updateFineGrainedEdgesAndRelationships(
          urn, upstreamLineage.getFineGrainedLineages(), edges, urnToRelationshipTypes);
    } else if (aspectSpec.getName().equals(Constants.INPUT_FIELDS_ASPECT_NAME)) {
      final InputFields inputFields = new InputFields(aspect.data());
      updateInputFieldEdgesAndRelationships(urn, inputFields, edges, urnToRelationshipTypes);
    } else if (aspectSpec.getName().equals(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)) {
      DataJobInputOutput dataJobInputOutput = new DataJobInputOutput(aspect.data());
      updateFineGrainedEdgesAndRelationships(
          urn, dataJobInputOutput.getFineGrainedLineages(), edges, urnToRelationshipTypes);
    }

    Map<RelationshipFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getRelationshipFieldSpecs(), true);

    for (Map.Entry<RelationshipFieldSpec, List<Object>> entry : extractedFields.entrySet()) {
      Set<String> relationshipTypes = urnToRelationshipTypes.getOrDefault(urn, new HashSet<>());
      relationshipTypes.add(entry.getKey().getRelationshipName());
      urnToRelationshipTypes.put(urn, relationshipTypes);
      final List<Edge> newEdges = extractGraphEdges(entry, aspect, urn, event, isNewAspectVersion);
      edges.addAll(newEdges);
    }
    return Pair.of(edges, urnToRelationshipTypes);
  }

  /**
   * Computes the differences between old and new aspects in terms of graph edges.
   *
   * @param urn The entity URN
   * @param aspectSpec The aspect specification
   * @param oldAspect The previous aspect value (can be null)
   * @param newAspect The new aspect value
   * @param event The metadata change log event
   * @return A triple containing lists of edges to add, remove, and update
   */
  public static EdgeDiff computeAspectEdgeDiff(
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate oldAspect,
      @Nonnull final RecordTemplate newAspect,
      @Nonnull final MetadataChangeLog event) {

    Pair<List<Edge>, HashMap<Urn, Set<String>>> oldEdgeAndRelationTypes = null;
    if (oldAspect != null) {
      oldEdgeAndRelationTypes =
          GraphIndexUtils.getEdgesAndRelationshipTypesFromAspect(
              urn, aspectSpec, oldAspect, event, false);
    }

    final List<Edge> oldEdges =
        oldEdgeAndRelationTypes != null
            ? oldEdgeAndRelationTypes.getFirst()
            : Collections.emptyList();
    final Set<Edge> oldEdgeSet = new HashSet<>(oldEdges);

    Pair<List<Edge>, HashMap<Urn, Set<String>>> newEdgeAndRelationTypes =
        GraphIndexUtils.getEdgesAndRelationshipTypesFromAspect(
            urn, aspectSpec, newAspect, event, true);

    final List<Edge> newEdges = newEdgeAndRelationTypes.getFirst();
    final Set<Edge> newEdgeSet = new HashSet<>(newEdges);

    // Edges to add
    final List<Edge> additiveDifference =
        newEdgeSet.stream().filter(edge -> !oldEdgeSet.contains(edge)).collect(Collectors.toList());

    // Edges to remove
    final List<Edge> subtractiveDifference =
        oldEdgeSet.stream().filter(edge -> !newEdgeSet.contains(edge)).collect(Collectors.toList());

    // Edges to update
    final List<Edge> mergedEdges = getMergedEdges(oldEdgeSet, newEdgeSet);

    return new EdgeDiff(additiveDifference, subtractiveDifference, mergedEdges);
  }

  private static List<Edge> getMergedEdges(final Set<Edge> oldEdgeSet, final Set<Edge> newEdgeSet) {
    final Map<Integer, Edge> oldEdgesMap =
        oldEdgeSet.stream()
            .map(edge -> Pair.of(edge.hashCode(), edge))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    final List<Edge> mergedEdges = new ArrayList<>();
    if (!oldEdgesMap.isEmpty()) {
      for (Edge newEdge : newEdgeSet) {
        if (oldEdgesMap.containsKey(newEdge.hashCode())) {
          final Edge oldEdge = oldEdgesMap.get(newEdge.hashCode());
          final Edge mergedEdge = GraphIndexUtils.mergeEdges(oldEdge, newEdge);
          mergedEdges.add(mergedEdge);
        }
      }
    }
    return mergedEdges;
  }
}
