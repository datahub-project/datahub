package com.linkedin.datahub.graphql.types.mappers;

import static com.linkedin.datahub.graphql.util.SearchInsightsUtil.*;
import static com.linkedin.metadata.utils.SearchUtil.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AggregationMetadata;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityPath;
import com.linkedin.datahub.graphql.generated.ExtraProperty;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.SearchSuggestion;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.utils.SearchUtils;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MapperUtils {

  private MapperUtils() {}

  public static SearchResult mapResult(
      @Nullable final QueryContext context, SearchEntity searchEntity) {
    return new SearchResult(
        UrnToEntityMapper.map(context, searchEntity.getEntity()),
        getInsightsFromFeatures(searchEntity.getFeatures()),
        getMatchedFieldEntry(context, searchEntity.getMatchedFields()),
        getExtraProperties(searchEntity.getExtraFields()));
  }

  private static List<ExtraProperty> getExtraProperties(@Nullable StringMap extraFields) {
    if (extraFields == null) {
      return List.of();
    } else {
      return extraFields.entrySet().stream()
          .map(
              entry -> {
                ExtraProperty extraProperty = new ExtraProperty();
                extraProperty.setName(entry.getKey());
                extraProperty.setValue(entry.getValue());
                return extraProperty;
              })
          .collect(Collectors.toList());
    }
  }

  public static FacetMetadata mapFacet(
      @Nullable final QueryContext context,
      com.linkedin.metadata.search.AggregationMetadata aggregationMetadata) {
    final FacetMetadata facetMetadata = new FacetMetadata();
    List<String> aggregationFacets =
        List.of(aggregationMetadata.getName().split(AGGREGATION_SEPARATOR_CHAR));
    List<Boolean> isEntityTypeFilter =
        aggregationFacets.stream()
            .map(facet -> facet.equals("entity") || facet.contains("_entityType"))
            .collect(Collectors.toList());
    if (aggregationMetadata.getEntity() != null) {
      facetMetadata.setEntity(UrnToEntityMapper.map(context, aggregationMetadata.getEntity()));
    }
    facetMetadata.setField(aggregationMetadata.getName());
    facetMetadata.setDisplayName(
        Optional.ofNullable(aggregationMetadata.getDisplayName())
            .orElse(aggregationMetadata.getName()));
    facetMetadata.setAggregations(
        aggregationMetadata.getFilterValues().stream()
            .map(
                filterValue ->
                    new AggregationMetadata(
                        convertFilterValue(filterValue.getValue(), isEntityTypeFilter),
                        filterValue.getFacetCount(),
                        filterValue.getEntity() == null
                            ? null
                            : UrnToEntityMapper.map(context, filterValue.getEntity())))
            .collect(Collectors.toList()));
    return facetMetadata;
  }

  public static String convertFilterValue(String filterValue, List<Boolean> isEntityTypeFilter) {
    String[] aggregations = filterValue.split(AGGREGATION_SEPARATOR_CHAR);
    return IntStream.range(0, aggregations.length)
        .mapToObj(
            idx ->
                idx < isEntityTypeFilter.size() && isEntityTypeFilter.get(idx)
                    ? EntityTypeMapper.getType(aggregations[idx]).toString()
                    : aggregations[idx])
        .collect(Collectors.joining(AGGREGATION_SEPARATOR_CHAR));
  }

  public static List<MatchedField> getMatchedFieldEntry(
      @Nullable final QueryContext context,
      List<com.linkedin.metadata.search.MatchedField> highlightMetadata) {
    return highlightMetadata.stream()
        .map(
            field -> {
              MatchedField matchedField = new MatchedField();
              matchedField.setName(field.getName());
              matchedField.setValue(field.getValue());
              if (SearchUtils.isUrn(field.getValue())) {
                try {
                  Urn urn = Urn.createFromString(field.getValue());
                  ValidationApiUtils.validateUrn(
                      context.getOperationContext().getEntityRegistry(), urn);
                  matchedField.setEntity(UrnToEntityMapper.map(context, urn));
                } catch (IllegalArgumentException | URISyntaxException e) {
                  log.debug("Failed to create urn from MatchedField value: {}", field.getValue());
                }
              }
              return matchedField;
            })
        .collect(Collectors.toList());
  }

  public static SearchSuggestion mapSearchSuggestion(
      com.linkedin.metadata.search.SearchSuggestion suggestion) {
    return new SearchSuggestion(
        suggestion.getText(), suggestion.getScore(), Math.toIntExact(suggestion.getFrequency()));
  }

  public static EntityPath mapPath(@Nullable final QueryContext context, UrnArray path) {
    EntityPath entityPath = new EntityPath();
    entityPath.setPath(
        path.stream().map(p -> UrnToEntityMapper.map(context, p)).collect(Collectors.toList()));
    return entityPath;
  }

  public static ResolvedAuditStamp createResolvedAuditStamp(AuditStamp auditStamp) {
    final ResolvedAuditStamp resolvedAuditStamp = new ResolvedAuditStamp();
    final CorpUser emptyCreatedUser = new CorpUser();
    emptyCreatedUser.setUrn(auditStamp.getActor().toString());
    resolvedAuditStamp.setActor(emptyCreatedUser);
    resolvedAuditStamp.setTime(auditStamp.getTime());
    return resolvedAuditStamp;
  }
}
