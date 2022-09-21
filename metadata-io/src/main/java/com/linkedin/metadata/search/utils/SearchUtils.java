package com.linkedin.metadata.search.utils;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.utils.SearchUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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

  public static final SearchResult EMPTY_SEARCH_RESULT =
      new SearchResult().setEntities(new SearchEntityArray(Collections.emptyList()))
          .setMetadata(new SearchResultMetadata())
          .setFrom(0)
          .setPageSize(0)
          .setNumEntities(0);

  private SearchUtils() {

  }

  public static Optional<String> getDocId(@Nonnull Urn urn) {
    try {
      return Optional.of(URLEncoder.encode(urn.toString(), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      log.error("Failed to encode the urn with error: {}", e.toString());
      return Optional.empty();
    }
  }

  /**
   * Validates the request params and create a request map out of it.
   *
   * @param requestParams the search request with fields and values
   * @return a request map
   */
  @Nonnull
  public static Map<String, String> getRequestMap(@Nullable Filter requestParams) {
    if (requestParams == null) {
      return Collections.emptyMap();
    }

    ConjunctiveCriterionArray disjunction = requestParams.getOr();

    if (disjunction.size() > 1) {
      throw new UnsupportedOperationException("To build request map, there must be only one conjunction group.");
    }

    CriterionArray criterionArray = disjunction.size() > 0 ? disjunction.get(0).getAnd() : new CriterionArray();

    criterionArray.forEach(criterion -> {
      if (!com.linkedin.metadata.query.filter.Condition.EQUAL.equals(criterion.getCondition())) {
        throw new UnsupportedOperationException("Unsupported condition: " + criterion.getCondition());
      }
    });

    return criterionArray.stream().collect(Collectors.toMap(Criterion::getField, Criterion::getValue));
  }

  static boolean isUrn(@Nonnull String value) {
    // TODO(https://github.com/datahub-project/datahub-gma/issues/51): This method is a bit of a hack to support searching for
    // URNs that have commas in them, while also using commas a delimiter for search. We should stop supporting commas
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

  @Nonnull
  public static Filter removeCriteria(@Nonnull Filter originalFilter, Predicate<Criterion> shouldRemove) {
    if (originalFilter.getOr() != null) {
      return new Filter().setOr(new ConjunctiveCriterionArray(originalFilter.getOr()
          .stream()
          .map(criteria -> removeCriteria(criteria, shouldRemove))
          .filter(criteria -> !criteria.getAnd().isEmpty())
          .collect(Collectors.toList())));
    }
    return originalFilter;
  }

  private static ConjunctiveCriterion removeCriteria(@Nonnull ConjunctiveCriterion conjunctiveCriterion,
      Predicate<Criterion> shouldRemove) {
    return new ConjunctiveCriterion().setAnd(new CriterionArray(conjunctiveCriterion.getAnd()
        .stream()
        .filter(criterion -> !shouldRemove.test(criterion))
        .collect(Collectors.toList())));
  }

  @SneakyThrows
  public static AggregationMetadata merge(AggregationMetadata one, AggregationMetadata two) {
    Map<String, Long> mergedMap =
        Stream.concat(one.getAggregations().entrySet().stream(), two.getAggregations().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
    return one.clone()
        .setDisplayName(two.getDisplayName() != two.getName() ? two.getDisplayName() : one.getDisplayName())
        .setAggregations(new LongMap(mergedMap))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(mergedMap)));
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
        new UrnArray(searchResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList())));
    return listResult;
  }
}
