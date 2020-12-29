package com.linkedin.datahub.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.AggregationMetadata;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.restli.common.CollectionResponse;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;


@Slf4j
public class SearchUtil {

  private static final ObjectMapper _OM = new ObjectMapper();

  private SearchUtil() {
  }

  @Nonnull
  public static <T extends RecordTemplate> Map<String, Object> toGraphQLSearchResponse(@Nonnull CollectionResponse<T> searchResponse) {

    final Map<String, Object> result = new HashMap<>();

    /*
     * Populate paging fields
     */
    if (!searchResponse.hasPaging()) {
      throw new RuntimeException("Invalid search response received. Unable to find paging details.");
    }
    result.put("start", searchResponse.getPaging().getStart());
    result.put("count", searchResponse.getPaging().getCount());
    result.put("total", searchResponse.getPaging().getTotal());

    /*
     * Populate elements
     */
    result.put("elements", toGraphQLElements(searchResponse.getElements()));

    /*
     * Populate facet metadata
     */
    final SearchResultMetadata searchResultMetadata = new SearchResultMetadata(searchResponse.getMetadataRaw());
    result.put("facets", toGraphQLFacetMetadata(searchResultMetadata.getSearchResultMetadatas()));

    return result;
  }

  @Nonnull
  public static Map<String, Object> toGraphQLAutoCompleteResponse(@Nonnull AutoCompleteResult autoCompleteResult) {
    final Map<String, Object> result = new HashMap<>();
    result.put("query", autoCompleteResult.getQuery());
    result.put("suggestions", autoCompleteResult.getSuggestions().data());
    return result;
  }

  private static <T extends RecordTemplate> List<Map<String, Object>> toGraphQLElements(List<T> elements) {
    return elements
        .stream()
        .map(RecordTemplate::data)
        .collect(Collectors.toList());
  }

  private static List<Map<String, Object>> toGraphQLFacetMetadata(AggregationMetadataArray aggregationMetadataArray) {
    List<Map<String, Object>> facetMetadataList = new ArrayList<>();
    for (AggregationMetadata aggregationMetadata : aggregationMetadataArray) {
      Map<String, Object> facetMetadata = new HashMap<>();
      facetMetadata.put("field", aggregationMetadata.getName());
      facetMetadata.put("aggregations", toGraphQLAggregationMetadata(aggregationMetadata.getAggregations()));
      facetMetadataList.add(facetMetadata);
    }
    return facetMetadataList;
  }

  private static List<Map<String, Object>> toGraphQLAggregationMetadata(LongMap facetAggregations) {
    List<Map<String, Object>> aggregationMetadataList = new ArrayList<>();
    for (Map.Entry<String, Long> entry : facetAggregations.entrySet()) {
      Map<String, Object> aggregationMetadata = new HashMap<>();
      aggregationMetadata.put("value", entry.getKey());
      aggregationMetadata.put("count", entry.getValue());
      aggregationMetadataList.add(aggregationMetadata);
    }
    return aggregationMetadataList;
  }
}