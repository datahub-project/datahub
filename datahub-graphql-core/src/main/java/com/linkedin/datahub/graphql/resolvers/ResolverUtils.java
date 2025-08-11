package com.linkedin.datahub.graphql.resolvers;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolverUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private static final Logger _logger = LoggerFactory.getLogger(ResolverUtils.class.getName());

  private ResolverUtils() {}

  @Nonnull
  public static <T> T bindArgument(Object argument, Class<T> clazz) {
    return MAPPER.convertValue(argument, clazz);
  }

  /**
   * Returns the string with the forward slash escaped More details on reserved characters in
   * Elasticsearch can be found at,
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
   */
  @Nonnull
  public static String escapeForwardSlash(@Nonnull String input) {
    if (input.contains("/")) {
      input = input.replace("/", "\\/");
    }
    return input;
  }

  @Nonnull
  public static Authentication getAuthentication(DataFetchingEnvironment environment) {
    return ((QueryContext) environment.getContext()).getAuthentication();
  }

  /**
   * @apiNote DO NOT use this method if the facet filters do not include `.keyword` suffix to ensure
   *     that it is matched against a keyword filter in ElasticSearch.
   * @param facetFilterInputs The list of facet filters inputs
   * @param validFacetFields The set of valid fields against which to filter for.
   * @return A map of filter definitions to be used in ElasticSearch.
   */
  @Nonnull
  public static Map<String, String> buildFacetFilters(
      @Nullable List<FacetFilterInput> facetFilterInputs, @Nonnull Set<String> validFacetFields) {
    if (facetFilterInputs == null) {
      return Collections.emptyMap();
    }

    final Map<String, String> facetFilters = new HashMap<>();

    facetFilterInputs.forEach(
        facetFilterInput -> {
          if (!validFacetFields.contains(facetFilterInput.getField())) {
            throw new ValidationException(
                String.format(
                    "Unrecognized facet with name %s provided", facetFilterInput.getField()));
          }
          if (!facetFilterInput.getValues().isEmpty()) {
            facetFilters.put(facetFilterInput.getField(), facetFilterInput.getValues().get(0));
          }
        });

    return facetFilters;
  }

  public static List<Criterion> criterionListFromAndFilter(List<FacetFilterInput> andFilters) {
    return andFilters != null && !andFilters.isEmpty()
        ? andFilters.stream()
            .map(filter -> criterionFromFilter(filter))
            .collect(Collectors.toList())
        : Collections.emptyList();
  }

  // In the case that user sends filters to be or-d together, we need to build a series of
  // conjunctive criterion
  // arrays, rather than just one for the AND case.
  public static ConjunctiveCriterionArray buildConjunctiveCriterionArrayWithOr(
      @Nonnull List<AndFilterInput> orFilters) {
    return new ConjunctiveCriterionArray(
        orFilters.stream()
            .map(
                orFilter -> {
                  CriterionArray andCriterionForOr =
                      new CriterionArray(criterionListFromAndFilter(orFilter.getAnd()));
                  return new ConjunctiveCriterion().setAnd(andCriterionForOr);
                })
            .collect(Collectors.toList()));
  }

  @Nullable
  public static Filter buildFilter(
      @Nullable List<FacetFilterInput> andFilters, @Nullable List<AndFilterInput> orFilters) {
    if ((andFilters == null || andFilters.isEmpty())
        && (orFilters == null || orFilters.isEmpty())) {
      return null;
    }

    // Or filters are the new default. We will check them first.
    // If we have OR filters, we need to build a series of CriterionArrays
    if (orFilters != null && !orFilters.isEmpty()) {
      return new Filter().setOr(buildConjunctiveCriterionArrayWithOr(orFilters));
    }

    // If or filters are not set, someone may be using the legacy and filters
    final List<Criterion> andCriterions = criterionListFromAndFilter(andFilters);
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(andCriterions))));
  }

  // Translates a FacetFilterInput (graphql input class) into Criterion (our internal model)
  public static Criterion criterionFromFilter(final FacetFilterInput filter) {

    final Condition condition;
    if (filter.getCondition() != null) {
      condition = Condition.valueOf(filter.getCondition().toString());
    } else {
      condition = Condition.EQUAL;
    }

    final List<String> values;
    if (filter.getValues() == null && filter.getValue() != null) {
      values = Collections.singletonList(filter.getValue());
    } else {
      values = filter.getValues();
    }

    return buildCriterion(filter.getField(), condition, filter.getNegated(), values);
  }

  public static Filter viewFilter(
      OperationContext opContext, ViewService viewService, String viewUrn) {
    if (viewUrn == null) {
      return null;
    }
    DataHubViewInfo viewInfo = resolveView(opContext, viewService, UrnUtils.getUrn(viewUrn));
    if (viewInfo == null) {
      return null;
    }
    Filter result = SearchUtils.combineFilters(null, viewInfo.getDefinition().getFilter());
    return result;
  }

  /**
   * Simply resolves the end time filter for the search across lineage query. If the start time is
   * provided, but end time is not provided, we will default to the current time.
   */
  public static Long getLineageEndTimeMillis(
      @Nullable Long startTimeMillis, @Nullable Long endTimeMillis) {
    if (endTimeMillis != null) {
      return endTimeMillis;
    }
    if (startTimeMillis != null) {
      return System.currentTimeMillis();
    }
    return null;
  }
}
