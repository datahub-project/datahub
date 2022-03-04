package com.linkedin.metadata.search.utils;

import com.datahub.util.ModelUtils;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.dao.BaseReadDAO;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class QueryUtils {

  public static final Filter EMPTY_FILTER = new Filter().setOr(new ConjunctiveCriterionArray());

  private QueryUtils() {
  }

  // Creates new Criterion with field and value, using EQUAL condition.
  @Nonnull
  public static Criterion newCriterion(@Nonnull String field, @Nonnull String value) {
    return newCriterion(field, value, Condition.EQUAL);
  }

  // Creates new Criterion with field, value and condition.
  @Nonnull
  public static Criterion newCriterion(@Nonnull String field, @Nonnull String value, @Nonnull Condition condition) {
    return new Criterion().setField(field).setValue(value).setCondition(condition);
  }

  // Creates new Filter from a map of Criteria by removing null-valued Criteria and using EQUAL condition (default).
  @Nonnull
  public static Filter newFilter(@Nullable Map<String, String> params) {
    if (params == null) {
      return EMPTY_FILTER;
    }
    CriterionArray criteria = params.entrySet()
        .stream()
        .filter(e -> Objects.nonNull(e.getValue()))
        .map(e -> newCriterion(e.getKey(), e.getValue()))
        .collect(Collectors.toCollection(CriterionArray::new));
    return new Filter().setOr(
        new ConjunctiveCriterionArray(ImmutableList.of(new ConjunctiveCriterion().setAnd(criteria))));
  }

  // Creates new Filter from a single Criterion with EQUAL condition (default).
  @Nonnull
  public static Filter newFilter(@Nonnull String field, @Nonnull String value) {
    return newFilter(Collections.singletonMap(field, value));
  }

  // Create singleton filter with one criterion
  @Nonnull
  public static Filter newFilter(@Nonnull Criterion criterion) {
    return new Filter().setOr(new ConjunctiveCriterionArray(
        ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(ImmutableList.of(criterion))))));
  }

  /**
   * Converts a set of aspect classes to a set of {@link AspectVersion} with the version all set to latest.
   */
  @Nonnull
  public static Set<AspectVersion> latestAspectVersions(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    return aspectClasses.stream()
        .map(aspectClass -> new AspectVersion().setAspect(ModelUtils.getAspectName(aspectClass))
            .setVersion(BaseReadDAO.LATEST_VERSION))
        .collect(Collectors.toSet());
  }

  /**
   * Create {@link RelationshipFilter} using filter conditions and relationship direction.
   *
   * @param field field to create a filter on
   * @param value field value to be filtered
   * @param relationshipDirection {@link RelationshipDirection} relationship direction
   * @return RelationshipFilter
   */
  @Nonnull
  public static RelationshipFilter createRelationshipFilter(@Nonnull String field, @Nonnull String value,
      @Nonnull RelationshipDirection relationshipDirection) {
    return createRelationshipFilter(newFilter(field, value), relationshipDirection);
  }

  /**
   * Create {@link RelationshipFilter} using filter and relationship direction.
   *
   * @param filter {@link Filter} filter
   * @param relationshipDirection {@link RelationshipDirection} relationship direction
   * @return RelationshipFilter
   */
  @Nonnull
  public static RelationshipFilter createRelationshipFilter(@Nonnull Filter filter,
      @Nonnull RelationshipDirection relationshipDirection) {
    return new RelationshipFilter().setOr(filter.getOr()).setDirection(relationshipDirection);
  }

  @Nonnull
  public static RelationshipFilter newRelationshipFilter(@Nonnull Filter filter,
      @Nonnull RelationshipDirection relationshipDirection) {
    return new RelationshipFilter().setOr(filter.getOr()).setDirection(relationshipDirection);
  }

  /**
   * Calculates the total page count.
   *
   * @param totalCount total count
   * @param size page size
   * @return total page count
   */
  public static int getTotalPageCount(int totalCount, int size) {
    if (size <= 0) {
      return 0;
    }
    return (int) Math.ceil((double) totalCount / size);
  }

  /**
   * Calculates whether there is more results.
   *
   * @param from offset from the first result you want to fetch
   * @param size page size
   * @param totalPageCount total page count
   * @return whether there's more results
   */
  public static boolean hasMore(int from, int size, int totalPageCount) {
    if (size <= 0) {
      return false;
    }
    return (from + size) / size < totalPageCount;
  }

  @Nonnull
  public static Filter getFilterFromCriteria(List<Criterion> criteria) {
    return new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria))));
  }
}
