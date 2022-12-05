package com.linkedin.metadata.kafka.hook;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.mxe.MetadataChangeLog;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class GraphIndexUtils {

  private GraphIndexUtils() { }

  @Nullable
  private static List<Urn> getActorList(@Nullable final String path, @Nonnull final RecordTemplate aspect) {
    if (path == null) {
      return null;
    }
    final PathSpec actorPathSpec = new PathSpec(path.split("/"));
    final Optional<Object> value = RecordUtils.getFieldValue(aspect, actorPathSpec);
    return (List<Urn>) value.orElse(null);
  }

  @Nullable
  private static List<Long> getTimestampList(@Nullable final String path, @Nonnull final RecordTemplate aspect) {
    if (path == null) {
      return null;
    }
    final PathSpec timestampPathSpec = new PathSpec(path.split("/"));
    final Optional<Object> value = RecordUtils.getFieldValue(aspect, timestampPathSpec);
    return (List<Long>) value.orElse(null);
  }

  @Nullable
  private static boolean isValueListValid(@Nullable final List<?> entryList, final int valueListSize) {
    if (entryList == null) {
      log.warn("Unable to get entry as entryList is null");
      return false;
    }
    if (valueListSize != entryList.size()) {
      log.warn("Unable to get entry for graph edge as values list and entry list have differing sizes");
      return false;
    }
    return true;
  }

  @Nullable
  private static Long getTimestamp(@Nullable final List<Long> timestampList, final int index, final int valueListSize) {
    if (isValueListValid(timestampList, valueListSize)) {
      return timestampList.get(index);
    }
    return null;
  }

  @Nullable
  private static Urn getActor(@Nullable final List<Urn> actorList, final int index, final int valueListSize) {
    if (isValueListValid(actorList, valueListSize)) {
      return actorList.get(index);
    }
    return null;
  }

  /**
   * Used to create new edges for the graph db, adding all the metadata associated with each edge based on the aspect.
   * Returns a list of Edges to be consumed by the graph service.
   */
  @Nonnull
  public static List<Edge> extractGraphEdges(
      @Nonnull final Map.Entry<RelationshipFieldSpec, List<Object>> extractedFieldsEntry,
      @Nonnull final RecordTemplate aspect,
      @Nonnull final Urn urn,
      @Nonnull final MetadataChangeLog event
  ) {
    final List<Edge> edgesToAdd = new ArrayList<>();
    final String createdOnPath = extractedFieldsEntry.getKey().getRelationshipAnnotation().getCreatedOn();
    final String createdActorPath = extractedFieldsEntry.getKey().getRelationshipAnnotation().getCreatedActor();
    final String updatedOnPath = extractedFieldsEntry.getKey().getRelationshipAnnotation().getUpdatedOn();
    final String updatedActorPath = extractedFieldsEntry.getKey().getRelationshipAnnotation().getUpdatedActor();

    final List<Long> createdOnList = getTimestampList(createdOnPath, aspect);
    final List<Urn> createdActorList = getActorList(createdActorPath, aspect);
    final List<Long> updatedOnList = getTimestampList(updatedOnPath, aspect);
    final List<Urn> updatedActorList = getActorList(updatedActorPath, aspect);

    int index = 0;
    for (Object fieldValue : extractedFieldsEntry.getValue()) {
      Long createdOn = getTimestamp(createdOnList, index, extractedFieldsEntry.getValue().size());
      Urn createdActor = getActor(createdActorList, index, extractedFieldsEntry.getValue().size());
      final Long updatedOn = getTimestamp(updatedOnList, index, extractedFieldsEntry.getValue().size());
      final Urn updatedActor = getActor(updatedActorList, index, extractedFieldsEntry.getValue().size());

      if (createdOn == null && event.hasSystemMetadata()) {
        createdOn = event.getSystemMetadata().getLastObserved();
      }
      if (createdActor == null && event.hasCreated()) {
        createdActor = event.getCreated().getActor();
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
                updatedActor
            )
        );
      } catch (URISyntaxException e) {
        log.error("Invalid destination urn: {}", fieldValue.toString(), e);
      }
      index++;
    }
    return edgesToAdd;
  }
}
