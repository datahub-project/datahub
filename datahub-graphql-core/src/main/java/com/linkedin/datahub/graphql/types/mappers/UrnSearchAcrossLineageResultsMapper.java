package com.linkedin.datahub.graphql.types.mappers;

import static com.linkedin.datahub.graphql.types.mappers.MapperUtils.*;
import static com.linkedin.datahub.graphql.util.SearchInsightsUtil.*;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FreshnessStats;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResult;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResults;
import com.linkedin.datahub.graphql.generated.SystemFreshness;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class UrnSearchAcrossLineageResultsMapper<T extends RecordTemplate, E extends Entity> {
  public static <T extends RecordTemplate, E extends Entity> SearchAcrossLineageResults map(
      @Nullable final QueryContext context, LineageSearchResult searchResult) {
    return new UrnSearchAcrossLineageResultsMapper<T, E>().apply(context, searchResult);
  }

  public SearchAcrossLineageResults apply(
      @Nullable final QueryContext context, LineageSearchResult input) {
    final SearchAcrossLineageResults result = new SearchAcrossLineageResults();

    result.setStart(input.getFrom());
    result.setCount(input.getPageSize());
    result.setTotal(input.getNumEntities());

    final SearchResultMetadata searchResultMetadata = input.getMetadata();
    result.setSearchResults(
        input.getEntities().stream().map(r -> mapResult(context, r)).collect(Collectors.toList()));
    result.setFacets(
        searchResultMetadata.getAggregations().stream()
            .map(f -> MapperUtils.mapFacet(context, f))
            .collect(Collectors.toList()));

    if (input.hasFreshness()) {
      FreshnessStats outputFreshness = new FreshnessStats();
      outputFreshness.setCached(input.getFreshness().isCached());
      outputFreshness.setSystemFreshness(
          input.getFreshness().getSystemFreshness().entrySet().stream()
              .map(
                  x ->
                      SystemFreshness.builder()
                          .setSystemName(x.getKey())
                          .setFreshnessMillis(x.getValue())
                          .build())
              .collect(Collectors.toList()));
      result.setFreshness(outputFreshness);
    }
    return result;
  }

  private SearchAcrossLineageResult mapResult(
      @Nullable final QueryContext context, LineageSearchEntity searchEntity) {
    return SearchAcrossLineageResult.builder()
        .setEntity(UrnToEntityMapper.map(context, searchEntity.getEntity()))
        .setInsights(getInsightsFromFeatures(searchEntity.getFeatures()))
        .setMatchedFields(getMatchedFieldEntry(context, searchEntity.getMatchedFields()))
        .setPaths(
            searchEntity.getPaths().stream()
                .map(p -> mapPath(context, p))
                .collect(Collectors.toList()))
        .setDegree(searchEntity.getDegree())
        .setDegrees(new ArrayList<>(searchEntity.getDegrees()))
        .setExplored(Boolean.TRUE.equals(searchEntity.isExplored()))
        .setIgnoredAsHop(Boolean.TRUE.equals(searchEntity.isIgnoredAsHop()))
        .setTruncatedChildren(Boolean.TRUE.equals(searchEntity.isTruncatedChildren()))
        .build();
  }
}
