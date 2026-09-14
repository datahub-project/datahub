package com.linkedin.metadata.graph;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;

import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

@Data
@AllArgsConstructor
public class GraphFilters {
  public static RelationshipFilter OUTGOING_FILTER =
      new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING);
  public static RelationshipFilter INCOMING_FILTER =
      new RelationshipFilter().setDirection(RelationshipDirection.INCOMING);

  public static GraphFilters incomingFilter(Filter sourceEntityFilter) {
    return new GraphFilters(sourceEntityFilter, EMPTY_FILTER, null, null, null, INCOMING_FILTER);
  }

  public static GraphFilters outgoingFilter(Filter sourceEntityFilter) {
    return new GraphFilters(sourceEntityFilter, EMPTY_FILTER, null, null, null, OUTGOING_FILTER);
  }

  public static GraphFilters from(
      Filter sourceEntityFilter,
      Collection<String> relationshipTypes,
      RelationshipFilter relationshipFilter) {
    return new GraphFilters(
        sourceEntityFilter,
        EMPTY_FILTER,
        null,
        null,
        new HashSet<>(relationshipTypes),
        relationshipFilter);
  }

  /** Direction is not important if no filters applied */
  public static GraphFilters ALL =
      new GraphFilters(EMPTY_FILTER, EMPTY_FILTER, null, null, null, INCOMING_FILTER);

  /**
   * Build a GraphFilters that matches all lineage edges across all entity types. The result uses
   * triplet-based filtering so that only (sourceType, destType, relType) combinations that are
   * actually annotated as lineage in the registry are matched.
   */
  public static GraphFilters forLineage(@Nonnull LineageRegistry lineageRegistry) {
    Set<Pair<String, LineageRegistry.EdgeInfo>> triplets = new HashSet<>();
    for (var entry : lineageRegistry.getLineageSpecs().entrySet()) {
      String entityType = entry.getKey();
      LineageRegistry.LineageSpec spec = entry.getValue();
      for (LineageRegistry.EdgeInfo edge :
          Stream.concat(spec.getUpstreamEdges().stream(), spec.getDownstreamEdges().stream())
              .collect(Collectors.toSet())) {
        triplets.add(Pair.of(entityType, edge));
      }
    }
    GraphFilters filters =
        new GraphFilters(EMPTY_FILTER, EMPTY_FILTER, null, null, Set.of(), INCOMING_FILTER);
    filters.setAllowedEdgeTriplets(triplets);
    return filters;
  }

  @Nonnull private Filter sourceEntityFilter;

  @Nonnull private Filter destinationEntityFilter;

  @Nullable private Set<String> sourceTypes;

  @Nullable private Set<String> destinationTypes;

  @Nonnull private Set<String> relationshipTypes;

  @Nonnull private RelationshipFilter relationshipFilter;

  @Nullable private RelationshipDirection relationshipDirection;

  /**
   * When set, filters edges by (sourceEntityType, destinationEntityType, relationshipType) triples
   * using OR logic — an edge matches if it belongs to ANY of the listed triplets. This gets applied
   * on top of the independent sourceTypes/destinationTypes/relationshipTypes filters, but generally
   * should not be used in combination with them. This is meant for more precise filters, e.g.
   * lineage edges where a relationship type may exist across entity types but only some
   * combinations constitute lineage.
   */
  @Nullable private Set<Pair<String, LineageRegistry.EdgeInfo>> allowedEdgeTriplets;

  public GraphFilters(
      @Nonnull Filter sourceEntityFilter,
      @Nonnull Filter destinationEntityFilter,
      @Nullable Set<String> sourceTypes,
      @Nullable Set<String> destinationTypes,
      @Nullable Set<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter) {
    this.sourceEntityFilter = sourceEntityFilter;
    this.destinationEntityFilter = destinationEntityFilter;
    this.sourceTypes = sourceTypes;
    this.destinationTypes = destinationTypes;
    this.relationshipTypes = relationshipTypes != null ? relationshipTypes : Set.of();
    this.relationshipFilter = relationshipFilter;
    this.relationshipDirection = relationshipFilter.getDirection();
  }

  public boolean isSourceTypesFilterEnabled() {
    return sourceTypes != null && !sourceTypes.isEmpty();
  }

  public boolean isDestinationTypesFilterEnabled() {
    return destinationTypes != null && !destinationTypes.isEmpty();
  }

  /**
   * @return ordered list of source types or null if no source types defined
   */
  @Nullable
  public List<String> getSourceTypesOrdered() {
    if (sourceTypes == null) {
      return null;
    }
    return sourceTypes.stream().sorted().collect(Collectors.toList());
  }

  /**
   * @return ordered list of destination types or null if no destination types defined
   */
  @Nullable
  public List<String> getDestinationTypesOrdered() {
    if (destinationTypes == null) {
      return null;
    }
    return destinationTypes.stream().sorted().collect(Collectors.toList());
  }

  /**
   * @return ordered list of relationship types, empty list if no types defined
   */
  @Nonnull
  public List<String> getRelationshipTypesOrdered() {
    return relationshipTypes.stream().sorted().collect(Collectors.toList());
  }

  public boolean noResultsByType() {
    return sourceTypes != null && sourceTypes.isEmpty()
        || destinationTypes != null && destinationTypes.isEmpty();
  }
}
