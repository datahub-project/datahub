package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.datahub.util.ModelUtils;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.util.Pair;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;

public class QueryUtils {

  public static final Filter EMPTY_FILTER = new Filter().setOr(new ConjunctiveCriterionArray());

  private QueryUtils() {}

  // Creates new Filter from a map of Criteria by removing null-valued Criteria and using EQUAL
  // condition (default).
  @Nonnull
  public static Filter newFilter(@Nullable Map<String, String> params) {
    if (params == null) {
      return EMPTY_FILTER;
    }
    CriterionArray criteria =
        params.entrySet().stream()
            .filter(e -> Objects.nonNull(e.getValue()))
            .map(e -> buildCriterion(e.getKey(), Condition.EQUAL, e.getValue()))
            .collect(Collectors.toCollection(CriterionArray::new));
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                ImmutableList.of(new ConjunctiveCriterion().setAnd(criteria))));
  }

  // Creates new Filter from a single Criterion with EQUAL condition (default).
  @Nonnull
  public static Filter newFilter(@Nonnull String field, @Nonnull String value) {
    return newFilter(Collections.singletonMap(field, value));
  }

  // Create singleton filter with one criterion
  @Nonnull
  public static Filter newFilter(@Nonnull Criterion criterion) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                ImmutableList.of(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(criterion))))));
  }

  @Nonnull
  public static Filter newDisjunctiveFilter(@Nonnull Criterion... orCriterion) {
    return new Filter()
        .setOr(
            Arrays.stream(orCriterion)
                .map(
                    criterion ->
                        new ConjunctiveCriterion()
                            .setAnd(new CriterionArray(ImmutableList.of(criterion))))
                .collect(Collectors.toCollection(ConjunctiveCriterionArray::new)));
  }

  @Nonnull
  public static Filter newDisjunctiveFilter(@Nonnull ConjunctiveCriterion... orCriterion) {
    return new Filter()
        .setOr(
            Arrays.stream(orCriterion)
                .collect(Collectors.toCollection(ConjunctiveCriterionArray::new)));
  }

  @Nonnull
  public static Filter newConjunctiveFilter(@Nonnull Criterion... andCriterion) {
    ConjunctiveCriterionArray orCriteria = new ConjunctiveCriterionArray();
    orCriteria.add(
        new ConjunctiveCriterion().setAnd(new CriterionArray(Arrays.asList(andCriterion))));
    return new Filter().setOr(orCriteria);
  }

  @Nonnull
  public static ConjunctiveCriterion add(
      @Nonnull ConjunctiveCriterion conjunctiveCriterion, @Nonnull Criterion element) {
    conjunctiveCriterion.getAnd().add(element);
    return conjunctiveCriterion;
  }

  @Nonnull
  public static Filter filterOrDefaultEmptyFilter(@Nullable Filter filter) {
    return filter != null ? filter : EMPTY_FILTER;
  }

  /**
   * Converts a set of aspect classes to a set of {@link AspectVersion} with the version all set to
   * latest.
   */
  @Nonnull
  public static Set<AspectVersion> latestAspectVersions(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    return aspectClasses.stream()
        .map(
            aspectClass ->
                new AspectVersion()
                    .setAspect(ModelUtils.getAspectName(aspectClass))
                    .setVersion(LATEST_VERSION))
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
  public static RelationshipFilter createRelationshipFilter(
      @Nonnull String field,
      @Nonnull String value,
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
  public static RelationshipFilter createRelationshipFilter(
      @Nonnull Filter filter, @Nonnull RelationshipDirection relationshipDirection) {
    return new RelationshipFilter().setOr(filter.getOr()).setDirection(relationshipDirection);
  }

  @Nonnull
  public static RelationshipFilter newRelationshipFilter(
      @Nonnull Filter filter, @Nonnull RelationshipDirection relationshipDirection) {
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
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(criteria))));
  }

  public static List<EntitySpec> getQueryByDefaultEntitySpecs(EntityRegistry entityRegistry) {
    return entityRegistry.getEntitySpecs().values().stream()
        .map(
            spec ->
                Pair.of(
                    spec,
                    spec.getSearchableFieldSpecs().stream()
                        .map(SearchableFieldSpec::getSearchableAnnotation)
                        .collect(Collectors.toList())))
        .filter(
            specPair ->
                specPair.getSecond().stream().anyMatch(SearchableAnnotation::isQueryByDefault))
        .map(Pair::getFirst)
        .collect(Collectors.toList());
  }

  /**
   * Build a filter with URNs and an optional existing filter
   *
   * @param urns urns to limit returns
   * @param inputFilters optional existing filter
   * @return filter with additional URN criterion
   */
  public static Filter buildFilterWithUrns(
      @Nonnull DataHubAppConfiguration appConfig,
      @Nonnull Set<Urn> urns,
      @Nullable Filter inputFilters) {
    // Determine which field is used for urn selection based on config
    boolean schemaFieldEnabled =
        appConfig.getMetadataChangeProposal().getSideEffects().getSchemaField().isEnabled();

    // Prevent increasing the query size by avoiding querying multiple fields with the
    // same URNs
    Criterion urnMatchCriterion =
        buildCriterion(
            "urn",
            Condition.EQUAL,
            urns.stream()
                .filter(
                    urn ->
                        !schemaFieldEnabled
                            || !urn.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME))
                .map(Object::toString)
                .collect(Collectors.toList()));

    Criterion schemaUrnAliasCriterion =
        buildCriterion(
            String.format("%s.keyword", SCHEMA_FIELD_ALIASES_ASPECT),
            Condition.EQUAL,
            urns.stream()
                .filter(
                    urn ->
                        schemaFieldEnabled && urn.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME))
                .map(Object::toString)
                .collect(Collectors.toList()));

    if (inputFilters == null || CollectionUtils.isEmpty(inputFilters.getOr())) {
      return QueryUtils.newDisjunctiveFilter(urnMatchCriterion, schemaUrnAliasCriterion);
    }

    // Add urn match criterion to each or clause
    inputFilters.setOr(
        inputFilters.getOr().stream()
            .flatMap(
                existingCriterion -> {
                  try {
                    return Stream.concat(
                        urnMatchCriterion.getValues().isEmpty()
                            ? Stream.of()
                            : Stream.of(
                                QueryUtils.add(existingCriterion.copy(), urnMatchCriterion)),
                        schemaUrnAliasCriterion.getValues().isEmpty()
                            ? Stream.of()
                            : Stream.of(
                                QueryUtils.add(existingCriterion.copy(), schemaUrnAliasCriterion)));
                  } catch (CloneNotSupportedException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toCollection(ConjunctiveCriterionArray::new)));

    return inputFilters;
  }
}
