package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.UrnArray;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.SearchUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

@Slf4j
public class SearchUtils {

  private SearchUtils() {}

  /**
   * Validates the request params and create a request map out of it.
   *
   * @param requestParams the search request with fields and values
   * @return a request map
   */
  @Nonnull
  public static Map<String, List<String>> getRequestMap(@Nullable Filter requestParams) {
    if (requestParams == null) {
      return Collections.emptyMap();
    }

    ConjunctiveCriterionArray disjunction = requestParams.getOr();

    if (disjunction.size() > 1) {
      throw new UnsupportedOperationException(
          "To build request map, there must be only one conjunction group.");
    }

    CriterionArray criterionArray =
        disjunction.size() > 0 ? disjunction.get(0).getAnd() : new CriterionArray();

    criterionArray.forEach(
        criterion -> {
          if (!com.linkedin.metadata.query.filter.Condition.EQUAL.equals(
              criterion.getCondition())) {
            throw new UnsupportedOperationException(
                "Unsupported condition: " + criterion.getCondition());
          }
        });

    return criterionArray.stream()
        .collect(Collectors.toMap(Criterion::getField, Criterion::getValues));
  }

  public static boolean isUrn(@Nonnull String value) {
    // TODO(https://github.com/datahub-project/datahub-gma/issues/51): This method is a bit of a
    // hack to support searching for
    // URNs that have commas in them, while also using commas a delimiter for search. We should stop
    // supporting commas
    // as delimiter, and then we can stop using this hack.
    return value.startsWith("urn:li:");
  }

  @Nonnull
  public static String toEntityType(@Nonnull Class c) {
    String result = c.getSimpleName().toLowerCase();
    if (result.endsWith("entity")) {
      result = result.substring(0, result.length() - 6);
    }
    return result;
  }

  @Nonnull
  public static String readResourceFile(@Nonnull Class clazz, @Nonnull String filePath) {
    try (InputStream inputStream = clazz.getClassLoader().getResourceAsStream(filePath)) {
      return IOUtils.toString(inputStream);
    } catch (IOException e) {
      log.error("Can't read file: " + filePath);
      throw new RuntimeException("Can't read file: " + filePath);
    }
  }

  public static Filter removeCriteria(
      @Nullable Filter originalFilter, Predicate<Criterion> shouldRemove) {
    if (originalFilter != null && originalFilter.getOr() != null) {
      return new Filter()
          .setOr(
              new ConjunctiveCriterionArray(
                  originalFilter.getOr().stream()
                      .map(criteria -> removeCriteria(criteria, shouldRemove))
                      .filter(criteria -> !criteria.getAnd().isEmpty())
                      .collect(Collectors.toList())));
    }
    return originalFilter;
  }

  private static ConjunctiveCriterion removeCriteria(
      @Nonnull ConjunctiveCriterion conjunctiveCriterion, Predicate<Criterion> shouldRemove) {
    return new ConjunctiveCriterion()
        .setAnd(
            new CriterionArray(
                conjunctiveCriterion.getAnd().stream()
                    .filter(criterion -> !shouldRemove.test(criterion))
                    .collect(Collectors.toList())));
  }

  @SneakyThrows
  public static AggregationMetadata merge(AggregationMetadata one, AggregationMetadata two) {
    Map<String, Long> mergedMap =
        Stream.concat(
                one.getAggregations().entrySet().stream(),
                two.getAggregations().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));

    // we want to make sure the values that were used in the filter are prioritized to appear in the
    // response aggregation
    Set<String> filteredValues =
        Stream.concat(one.getFilterValues().stream(), two.getFilterValues().stream())
            .filter(val -> val.isFiltered())
            .map(val -> val.getValue())
            .collect(Collectors.toSet());

    return one.clone()
        .setDisplayName(
            two.getDisplayName() != two.getName() ? two.getDisplayName() : one.getDisplayName())
        .setAggregations(new LongMap(mergedMap))
        .setFilterValues(
            new FilterValueArray(SearchUtil.convertToFilters(mergedMap, filteredValues)));
  }

  public static ListResult toListResult(final SearchResult searchResult) {
    if (searchResult == null) {
      return null;
    }
    final ListResult listResult = new ListResult();
    listResult.setStart(searchResult.getFrom());
    listResult.setCount(searchResult.getPageSize());
    listResult.setTotal(searchResult.getNumEntities());
    listResult.setEntities(
        new UrnArray(
            searchResult.getEntities().stream()
                .map(SearchEntity::getEntity)
                .collect(Collectors.toList())));
    return listResult;
  }

  @SneakyThrows
  public static SearchFlags applyDefaultSearchFlags(
      @Nullable SearchFlags inputFlags, @Nullable String query, @Nonnull SearchFlags defaultFlags) {
    SearchFlags finalSearchFlags = inputFlags != null ? inputFlags : defaultFlags.copy();
    if (!finalSearchFlags.hasFulltext() || finalSearchFlags.isFulltext() == null) {
      finalSearchFlags.setFulltext(defaultFlags.isFulltext());
    }
    if (query == null || Set.of("*", "").contains(query)) {
      // No highlighting if no query string
      finalSearchFlags.setSkipHighlighting(true);
    } else if (!finalSearchFlags.hasSkipHighlighting()
        || finalSearchFlags.isSkipHighlighting() == null) {
      finalSearchFlags.setSkipHighlighting(defaultFlags.isSkipHighlighting());
    }
    if (!finalSearchFlags.hasSkipAggregates() || finalSearchFlags.isSkipAggregates() == null) {
      finalSearchFlags.setSkipAggregates(defaultFlags.isSkipAggregates());
    }
    if (!finalSearchFlags.hasMaxAggValues() || finalSearchFlags.getMaxAggValues() == null) {
      finalSearchFlags.setMaxAggValues(defaultFlags.getMaxAggValues());
    }
    if (!finalSearchFlags.hasSkipCache() || finalSearchFlags.isSkipCache() == null) {
      finalSearchFlags.setSkipCache(defaultFlags.isSkipCache());
    }
    if (!finalSearchFlags.hasIncludeSoftDeleted()
        || finalSearchFlags.isIncludeSoftDeleted() == null) {
      finalSearchFlags.setIncludeSoftDeleted(defaultFlags.isIncludeSoftDeleted());
    }
    if (!finalSearchFlags.hasIncludeRestricted()
        || finalSearchFlags.isIncludeRestricted() == null) {
      finalSearchFlags.setIncludeRestricted(defaultFlags.isIncludeRestricted());
    }
    if ((!finalSearchFlags.hasGroupingSpec() || finalSearchFlags.getGroupingSpec() == null)
        && (defaultFlags.getGroupingSpec() != null)) {
      finalSearchFlags.setGroupingSpec(defaultFlags.getGroupingSpec());
    }
    return finalSearchFlags;
  }

  /**
   * Returns true if the search flags contain a grouping spec that requires conversion of schema
   * field entity to dataset entity.
   *
   * @param searchFlags the search flags
   * @return true if the search flags contain a grouping spec that requires conversion of schema
   *     field entity to dataset entity.
   */
  public static boolean convertSchemaFieldToDataset(@Nullable SearchFlags searchFlags) {
    return (searchFlags != null)
        && (searchFlags.getGroupingSpec() != null)
        && (searchFlags.getGroupingSpec().getGroupingCriteria().stream()
            .anyMatch(
                grouping ->
                    grouping.getBaseEntityType().equals(SCHEMA_FIELD_ENTITY_NAME)
                        && grouping.getGroupingEntityType().equals(DATASET_ENTITY_NAME)));
  }
}
