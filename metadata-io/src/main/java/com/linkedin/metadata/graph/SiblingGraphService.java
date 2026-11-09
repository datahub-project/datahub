package com.linkedin.metadata.graph;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SiblingGraphService {

  /**
   * Upper bound on the members of one sibling group counted together. Every member goes into the
   * count request several times, so this also bounds the request size.
   */
  public static final int MAX_GROUP_MEMBERS = 1_000;

  /** Sibling aspects read per batch while walking a group. */
  public static final int GROUP_READ_BATCH_SIZE = 500;

  private final EntityService _entityService;
  private final GraphService _graphService;

  /** Keeps the capped-group warning to about once a minute per server. */
  private final RateLimiter _capWarning = RateLimiter.create(1.0 / 60);

  @Nonnull
  public EntityLineageResult getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageDirection direction,
      int offset,
      @Nullable Integer count,
      int maxHops) {
    return getLineage(opContext, entityUrn, direction, offset, count, maxHops, false, false);
  }

  @Nonnull
  public EntityLineageResult getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageDirection direction,
      int offset,
      @Nullable Integer count,
      int maxHops,
      boolean separateSiblings,
      boolean includeGhostEntities) {
    return getLineage(
        opContext,
        entityUrn,
        direction,
        offset,
        count,
        maxHops,
        separateSiblings,
        includeGhostEntities,
        new HashSet<>());
  }

  /**
   * Traverse from the entityUrn towards the input direction up to maxHops number of hops Abstracts
   * away the concept of relationship types
   *
   * <p>Unless overridden, it uses the lineage registry to fetch valid edge types and queries for
   * them
   */
  @Nonnull
  public EntityLineageResult getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageDirection direction,
      int offset,
      @Nullable Integer count,
      int maxHops,
      boolean separateSiblings,
      boolean includeGhostEntities,
      @Nonnull Set<Urn> visitedUrns) {

    count = ConfigUtils.applyLimit(_graphService.getGraphServiceConfig(), count);

    if (separateSiblings) {
      return ValidationUtils.validateEntityLineageResult(
          opContext,
          _graphService.getLineage(opContext, entityUrn, direction, offset, count, maxHops),
          _entityService,
          includeGhostEntities);
    }

    if (maxHops > 1) {
      throw new UnsupportedOperationException(
          String.format(
              "More than 1 hop is not supported for %s", this.getClass().getSimpleName()));
    }

    EntityLineageResult entityLineage =
        _graphService.getLineage(opContext, entityUrn, direction, offset, count, maxHops);

    Siblings siblingAspectOfEntity =
        (Siblings) _entityService.getLatestAspect(opContext, entityUrn, SIBLINGS_ASPECT_NAME);

    // if you have siblings, we want to fetch their lineage too and merge it in
    if (siblingAspectOfEntity != null && siblingAspectOfEntity.hasSiblings()) {
      UrnArray siblingUrns = siblingAspectOfEntity.getSiblings();
      Set<Urn> allSiblingsInGroup = new HashSet<>(siblingUrns);
      allSiblingsInGroup.add(entityUrn);

      // remove your siblings from your lineage
      entityLineage =
          filterLineageResultFromSiblings(
              opContext, entityUrn, allSiblingsInGroup, entityLineage, null, includeGhostEntities);

      // Update offset and count to fetch the correct number of edges from the next sibling node
      offset = Math.max(0, offset - entityLineage.getTotal());
      count = Math.max(0, count - entityLineage.getRelationships().size());

      visitedUrns.add(entityUrn);
      // iterate through each sibling and include their lineage in the bunch
      for (Urn siblingUrn : siblingUrns) {
        if (visitedUrns.contains(siblingUrn)) {
          continue;
        }
        // need to call siblingGraphService to get sibling results for this sibling entity in case
        // there is more than one sibling
        EntityLineageResult nextEntityLineage =
            filterLineageResultFromSiblings(
                opContext,
                siblingUrn,
                allSiblingsInGroup,
                getLineage(
                    opContext,
                    siblingUrn,
                    direction,
                    offset,
                    count,
                    maxHops,
                    false,
                    includeGhostEntities,
                    visitedUrns),
                entityLineage,
                includeGhostEntities);

        // Update offset and count to fetch the correct number of edges from the next sibling node
        offset = Math.max(0, offset - nextEntityLineage.getTotal());
        count = Math.max(0, count - nextEntityLineage.getCount() - entityLineage.getCount());

        entityLineage.setFiltered(getFiltered(entityLineage) + getFiltered(nextEntityLineage));
        entityLineage = nextEntityLineage;
      }
      ;
    }

    return ValidationUtils.validateEntityLineageResult(
        opContext, entityLineage, _entityService, includeGhostEntities);
  }

  private int getFiltered(@Nullable EntityLineageResult entityLineageResult) {
    return (entityLineageResult != null && entityLineageResult.getFiltered() != null
        ? entityLineageResult.getFiltered()
        : 0);
  }

  /**
   * Counts an entity's one-hop lineage without fetching edges. {@code total} is the number of
   * distinct neighbours of the entity and, unless {@code separateSiblings}, of its siblings: a
   * neighbour shared by several members of the group counts once. {@code filtered} applies the
   * walk's post-processing (non-primary siblings of other results, then ghosts) to a sample of up
   * to {@code count} of those neighbours. Relationships are left empty.
   *
   * <p>This deliberately differs from {@link #getLineage}. The walk adds up each member's edge
   * total and removes only the duplicates it sees within one page, so for a sibling group whose
   * lineage is larger than a page its {@code total - filtered} over-counts shared neighbours and
   * depends on the page size. When the group's lineage fits in one page the two give the same
   * {@code total - filtered}. {@code total} on its own differs whenever members share neighbours:
   * the walk counts edges, this counts distinct entities.
   */
  @Nonnull
  public EntityLineageResult getLineageCount(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageDirection direction,
      final int start,
      @Nullable Integer count,
      boolean separateSiblings,
      boolean includeGhostEntities) {
    final int sampleSize = ConfigUtils.applyLimit(_graphService.getGraphServiceConfig(), count);

    final Set<Urn> group =
        separateSiblings ? Set.of(entityUrn) : siblingGroup(opContext, entityUrn);

    final LineageCountResult counted =
        _graphService.getLineageCount(
            opContext,
            group,
            LineageGraphFilters.forEntityType(
                _graphService.getLineageRegistry(), entityUrn.getEntityType(), direction),
            sampleSize);

    final Set<Urn> sample = new LinkedHashSet<>(counted.getSample());
    int filtered = 0;
    if (!separateSiblings && !sample.isEmpty()) {
      Set<Urn> collapsed = nonPrimarySiblingsWithinResults(opContext, sample);
      filtered += collapsed.size();
      sample.removeAll(collapsed);
    }
    if (!includeGhostEntities && !sample.isEmpty()) {
      filtered += sample.size() - _entityService.exists(opContext, sample, false).size();
    }

    return new EntityLineageResult()
        .setStart(start)
        // count is the size of the returned relationships, which a totals-only result leaves empty.
        .setCount(0)
        .setTotal(counted.getTotal())
        .setFiltered(filtered)
        .setRelationships(new LineageRelationshipArray());
  }

  /**
   * {@code urn} and every dataset reachable from it through sibling lists, transitively, as
   * getLineage reaches them when it recurses into each sibling. Lists are not always symmetric: the
   * dbt sibling hook sets a dbt node's list to its warehouse table alone but appends to the table's
   * list, so two dbt nodes on one table are only connected through the table.
   *
   * <p>Sibling aspects are read one hop at a time in batches, and the walk stops growing at {@link
   * #MAX_GROUP_MEMBERS}. In a group over the cap, members beyond it are not excluded from its
   * counts.
   */
  @Nonnull
  public Set<Urn> siblingGroup(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    return Collections.unmodifiableSet(walkGroup(opContext, urn).primary().keySet());
  }

  /** A member of a sibling group and whether its own siblings aspect marks it primary. */
  public record SiblingGroupMember(@Nonnull Urn urn, boolean primary) {}

  /**
   * The other members of a sibling group. {@code truncated} is true when the group is larger than
   * {@link #MAX_GROUP_MEMBERS}, so {@code members} is only part of it.
   */
  public record SiblingGroupMembers(@Nonnull List<SiblingGroupMember> members, boolean truncated) {}

  /**
   * The other members of {@code urn}'s sibling group (see {@link #siblingGroup}), primary first and
   * then in URN order, leaving out soft-deleted members (which still count toward the cap).
   */
  @Nonnull
  public SiblingGroupMembers siblingGroupMembers(
      @Nonnull OperationContext opContext, @Nonnull Urn urn) {
    final GroupWalk walk = walkGroup(opContext, urn);
    final Map<Urn, Boolean> group = new LinkedHashMap<>(walk.primary());
    group.remove(urn);
    if (group.isEmpty()) {
      return new SiblingGroupMembers(List.of(), walk.truncated());
    }
    // A capped walk stops before reading every member's own aspect; read those for their flag.
    final List<Urn> unread =
        walk.unread().stream().filter(group::containsKey).collect(Collectors.toList());
    for (List<Urn> batch : Iterables.partition(unread, GROUP_READ_BATCH_SIZE)) {
      final List<Siblings> aspects = siblingAspects(opContext, batch);
      for (int i = 0; i < batch.size(); i++) {
        group.put(batch.get(i), isPrimary(aspects.get(i)));
      }
    }
    final Set<Urn> existing = _entityService.exists(opContext, group.keySet(), false);
    return new SiblingGroupMembers(
        group.entrySet().stream()
            .filter(member -> existing.contains(member.getKey()))
            .map(member -> new SiblingGroupMember(member.getKey(), member.getValue()))
            .sorted(
                Comparator.comparing((SiblingGroupMember member) -> !member.primary())
                    .thenComparing(member -> member.urn().toString()))
            .collect(Collectors.toList()),
        walk.truncated());
  }

  /**
   * A walked group: each member's primary flag (read from its own aspect, or {@code false} when not
   * read), the members whose aspect wasn't read because the walk stopped at the cap, and whether it
   * stopped there.
   */
  private record GroupWalk(Map<Urn, Boolean> primary, Set<Urn> unread, boolean truncated) {}

  @Nonnull
  private GroupWalk walkGroup(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    final Map<Urn, Boolean> group = new LinkedHashMap<>(Map.of(urn, false));
    final Set<Urn> read = new HashSet<>();
    boolean truncated = false;
    Set<Urn> frontier = Set.of(urn);
    walk:
    while (!frontier.isEmpty()) {
      final Set<Urn> next = new LinkedHashSet<>();
      for (List<Urn> batch : Iterables.partition(frontier, GROUP_READ_BATCH_SIZE)) {
        final List<Siblings> aspects = siblingAspects(opContext, batch);
        for (int i = 0; i < batch.size(); i++) {
          final Siblings aspect = aspects.get(i);
          read.add(batch.get(i));
          if (aspect == null) {
            continue;
          }
          group.put(batch.get(i), isPrimary(aspect));
          if (!aspect.hasSiblings()) {
            continue;
          }
          for (Urn sibling : aspect.getSiblings()) {
            if (group.containsKey(sibling)) {
              continue;
            }
            if (group.size() >= MAX_GROUP_MEMBERS) {
              if (_capWarning.tryAcquire()) {
                log.warn(
                    "Sibling group of {} has more than {} members; counting lineage for the first {}",
                    urn,
                    MAX_GROUP_MEMBERS,
                    MAX_GROUP_MEMBERS);
              }
              truncated = true;
              break walk;
            }
            group.put(sibling, false);
            next.add(sibling);
          }
        }
      }
      frontier = next;
    }
    final Set<Urn> unread = new LinkedHashSet<>(group.keySet());
    unread.removeAll(read);
    return new GroupWalk(group, unread, truncated);
  }

  /** A legacy aspect may lack the required {@code primary} field; treat that as not primary. */
  private static boolean isPrimary(@Nullable Siblings siblings) {
    return siblings != null && Boolean.TRUE.equals(siblings.isPrimary(GetMode.NULL));
  }

  /**
   * The siblings aspect of each of {@code urns}, in batch order ({@code null} where there is none),
   * from one batched read.
   */
  @Nonnull
  private List<Siblings> siblingAspects(
      @Nonnull OperationContext opContext, @Nonnull List<Urn> urns) {
    final Map<Urn, List<RecordTemplate>> aspects =
        _entityService.getLatestAspects(
            opContext, new HashSet<>(urns), ImmutableSet.of(SIBLINGS_ASPECT_NAME), false);
    final List<Siblings> result = new ArrayList<>(urns.size());
    for (Urn urn : urns) {
      result.add(
          aspects.getOrDefault(urn, List.of()).stream()
              .filter(aspect -> aspect instanceof Siblings)
              .map(aspect -> (Siblings) aspect)
              .findFirst()
              .orElse(null));
    }
    return result;
  }

  /**
   * Among {@code resultUrns}, the non-primary siblings that have a sibling in the same set. A
   * combined result shows a sibling pair once, as its primary, so these are dropped.
   */
  private Set<Urn> nonPrimarySiblingsWithinResults(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> resultUrns) {
    final Map<Urn, List<RecordTemplate>> siblingAspects =
        _entityService.getLatestAspects(
            opContext, resultUrns, ImmutableSet.of(SIBLINGS_ASPECT_NAME), false);
    return resultUrns.stream()
        .filter(
            urn -> {
              Optional<RecordTemplate> optionalSiblingsAspect =
                  siblingAspects.getOrDefault(urn, List.of()).stream()
                      .filter(aspect -> aspect instanceof Siblings)
                      .findAny();
              if (optionalSiblingsAspect.isEmpty()) {
                return false;
              }
              final Siblings siblingsAspect = (Siblings) optionalSiblingsAspect.get();
              if (siblingsAspect.isPrimary() || !siblingsAspect.hasSiblings()) {
                return false;
              }
              return siblingsAspect.getSiblings().stream().anyMatch(resultUrns::contains);
            })
        .collect(Collectors.toSet());
  }

  // takes a lineage result and removes any nodes that are siblings of some other node already in
  // the result
  private EntityLineageResult filterLineageResultFromSiblings(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<Urn> allSiblingsInGroup,
      @Nonnull final EntityLineageResult entityLineageResult,
      @Nullable final EntityLineageResult existingResult,
      boolean includeGhostEntities) {
    int numFiltered = 0;

    // 1) remove the source entities siblings from this entity's downstreams
    final Map<Boolean, List<LineageRelationship>> partitionedFilteredRelationships =
        entityLineageResult.getRelationships().stream()
            .collect(
                Collectors.partitioningBy(
                    lineageRelationship ->
                        !allSiblingsInGroup.contains(lineageRelationship.getEntity())
                            || lineageRelationship.getEntity().equals(urn)));
    numFiltered += partitionedFilteredRelationships.get(Boolean.FALSE).size();

    final List<LineageRelationship> filteredRelationships =
        partitionedFilteredRelationships.get(Boolean.TRUE);

    // 2) filter out existing lineage to avoid duplicates in our combined result
    final Set<Urn> existingUrns =
        existingResult != null
            ? existingResult.getRelationships().stream()
                .map(LineageRelationship::getEntity)
                .collect(Collectors.toSet())
            : new HashSet<>();

    Map<Boolean, List<LineageRelationship>> partitionedUniqueFilteredRelationships =
        filteredRelationships.stream()
            .collect(
                Collectors.partitioningBy(
                    lineageRelationship ->
                        !existingUrns.contains(lineageRelationship.getEntity())));
    numFiltered += partitionedUniqueFilteredRelationships.get(Boolean.FALSE).size();

    List<LineageRelationship> uniqueFilteredRelationships =
        partitionedUniqueFilteredRelationships.get(Boolean.TRUE);

    // 3) combine this entity's lineage with the lineage we've already seen
    final List<LineageRelationship> combinedResults =
        Stream.concat(
                uniqueFilteredRelationships.stream(),
                existingResult != null
                    ? existingResult.getRelationships().stream()
                    : ImmutableList.<LineageRelationship>of().stream())
            .collect(Collectors.toList());

    // 4) fetch the siblings of each lineage result and, 5) if you are not primary & your sibling
    // is in the results, filter yourself out of the return set
    final Set<Urn> combinedResultUrns =
        combinedResults.stream().map(LineageRelationship::getEntity).collect(Collectors.toSet());
    final Set<Urn> collapsedUrns = nonPrimarySiblingsWithinResults(opContext, combinedResultUrns);

    Map<Boolean, List<LineageRelationship>> partitionedFilteredSiblings =
        combinedResults.stream()
            .collect(
                Collectors.partitioningBy(result -> !collapsedUrns.contains(result.getEntity())));

    numFiltered += partitionedFilteredSiblings.get(Boolean.FALSE).size();
    uniqueFilteredRelationships = partitionedFilteredSiblings.get(Boolean.TRUE);

    EntityLineageResult combinedLineageResult = new EntityLineageResult();
    combinedLineageResult.setStart(entityLineageResult.getStart());
    combinedLineageResult.setRelationships(
        new LineageRelationshipArray(uniqueFilteredRelationships));
    combinedLineageResult.setTotal(
        entityLineageResult.getTotal() + (existingResult != null ? existingResult.getTotal() : 0));
    combinedLineageResult.setCount(uniqueFilteredRelationships.size());
    combinedLineageResult.setFiltered(
        numFiltered + getFiltered(existingResult) + getFiltered(entityLineageResult));
    return ValidationUtils.validateEntityLineageResult(
        opContext, combinedLineageResult, _entityService, includeGhostEntities);
  }
}
