package com.linkedin.datahub.graphql.types.mappers;

import static com.linkedin.datahub.graphql.types.mappers.MapperUtils.*;
import static com.linkedin.datahub.graphql.util.SearchInsightsUtil.*;

import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPath;
import com.linkedin.datahub.graphql.generated.ScrollAcrossLineageResults;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResult;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class UrnScrollAcrossLineageResultsMapper<T extends RecordTemplate, E extends Entity> {
  public static <T extends RecordTemplate, E extends Entity> ScrollAcrossLineageResults map(
      @Nullable final QueryContext context, LineageScrollResult searchResult) {
    return new UrnScrollAcrossLineageResultsMapper<T, E>().apply(context, searchResult);
  }

  public ScrollAcrossLineageResults apply(
      @Nullable final QueryContext context, LineageScrollResult input) {
    final ScrollAcrossLineageResults result = new ScrollAcrossLineageResults();

    result.setNextScrollId(input.getScrollId());
    result.setCount(input.getPageSize());
    result.setTotal(input.getNumEntities());

    final SearchResultMetadata searchResultMetadata = input.getMetadata();
    result.setSearchResults(
        input.getEntities().stream().map(r -> mapResult(context, r)).collect(Collectors.toList()));
    result.setFacets(
        searchResultMetadata.getAggregations().stream()
            .map(f -> mapFacet(context, f))
            .collect(Collectors.toList()));

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
        .build();
  }

  private EntityPath mapPath(@Nullable final QueryContext context, UrnArray path) {
    EntityPath entityPath = new EntityPath();
    entityPath.setPath(
        path.stream().map(p -> UrnToEntityMapper.map(context, p)).collect(Collectors.toList()));
    return entityPath;
  }
}
