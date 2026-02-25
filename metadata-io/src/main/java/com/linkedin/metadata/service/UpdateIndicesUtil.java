package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Utility class for grouping and processing metadata change log events. */
public class UpdateIndicesUtil {

  /**
   * Groups events by URN while preserving event ordering. This is useful for batching operations
   * where events for the same URN can be processed together efficiently.
   *
   * @param events the stream of MCLItem events to group
   * @return a LinkedHashMap with URN as key and list of events as value, preserving order
   */
  public static LinkedHashMap<Urn, List<MCLItem>> groupEventsByUrn(
      @Nonnull Stream<MCLItem> events) {
    LinkedHashMap<Urn, List<MCLItem>> groupedEvents = new LinkedHashMap<>();

    events.forEach(
        event -> {
          Urn urn = event.getUrn();
          groupedEvents.computeIfAbsent(urn, k -> new ArrayList<>()).add(event);
        });

    return groupedEvents;
  }

  /**
   * Extracts the entity and aspect specifications from an MCLItem event.
   *
   * @param event the MCLItem event
   * @return a Pair containing the EntitySpec and AspectSpec
   * @throws RuntimeException if the aspect spec cannot be found
   */
  public static Pair<EntitySpec, AspectSpec> extractSpecPair(@Nonnull final MCLItem event) {
    final EntitySpec entitySpec = event.getEntitySpec();
    final Urn urn = event.getUrn();

    AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());
    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve Aspect Spec for entity with name %s, aspect with name %s. Cannot update indices for MCL.",
              urn.getEntityType(), event.getAspectName()));
    }

    return Pair.of(entitySpec, aspectSpec);
  }

  /**
   * Determines if the given aspect specification represents a key aspect deletion.
   *
   * @param specPair the entity and aspect specification pair
   * @return true if this is a key aspect deletion, false otherwise
   */
  public static boolean isDeletingKey(Pair<EntitySpec, AspectSpec> specPair) {
    return specPair.getSecond().getName().equals(specPair.getFirst().getKeyAspectName());
  }
}
