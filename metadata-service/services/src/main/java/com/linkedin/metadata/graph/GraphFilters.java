package com.linkedin.metadata.graph;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;

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

  @Nonnull private Filter sourceEntityFilter;

  @Nonnull private Filter destinationEntityFilter;

  @Nullable private Set<String> sourceTypes;

  @Nullable private Set<String> destinationTypes;

  @Nonnull private Set<String> relationshipTypes;

  @Nonnull private RelationshipFilter relationshipFilter;

  @Nullable private RelationshipDirection relationshipDirection;

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
