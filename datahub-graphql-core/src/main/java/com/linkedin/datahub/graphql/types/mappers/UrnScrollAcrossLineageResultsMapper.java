package com.linkedin.datahub.graphql.types.mappers;

import static com.linkedin.datahub.graphql.types.mappers.MapperUtils.*;
import static com.linkedin.datahub.graphql.util.SearchInsightsUtil.*;

import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPath;
import com.linkedin.datahub.graphql.generated.ScrollAcrossLineageResults;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResult;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.stream.Collectors;

public class UrnScrollAcrossLineageResultsMapper<T extends RecordTemplate, E extends Entity> {
  public static <T extends RecordTemplate, E extends Entity> ScrollAcrossLineageResults map(
      LineageScrollResult searchResult) {
    return new UrnScrollAcrossLineageResultsMapper<T, E>().apply(searchResult);
  }

  public ScrollAcrossLineageResults apply(LineageScrollResult input) {
    final ScrollAcrossLineageResults result = new ScrollAcrossLineageResults();

    result.setNextScrollId(input.getScrollId());
    result.setCount(input.getPageSize());
    result.setTotal(input.getNumEntities());

    final SearchResultMetadata searchResultMetadata = input.getMetadata();
    result.setSearchResults(
        input.getEntities().stream().map(this::mapResult).collect(Collectors.toList()));
    result.setFacets(
        searchResultMetadata.getAggregations().stream()
            .map(MapperUtils::mapFacet)
            .collect(Collectors.toList()));

    return result;
  }

  private SearchAcrossLineageResult mapResult(LineageSearchEntity searchEntity) {
    return SearchAcrossLineageResult.builder()
        .setEntity(UrnToEntityMapper.map(searchEntity.getEntity()))
        .setInsights(getInsightsFromFeatures(searchEntity.getFeatures()))
        .setMatchedFields(getMatchedFieldEntry(searchEntity.getMatchedFields()))
        .setPaths(searchEntity.getPaths().stream().map(this::mapPath).collect(Collectors.toList()))
        .setDegree(searchEntity.getDegree())
        .build();
  }

  private EntityPath mapPath(UrnArray path) {
    EntityPath entityPath = new EntityPath();
    entityPath.setPath(path.stream().map(UrnToEntityMapper::map).collect(Collectors.toList()));
    return entityPath;
  }
}
