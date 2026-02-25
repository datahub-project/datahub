package com.linkedin.metadata.graph;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphIndexUtils {

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
}
