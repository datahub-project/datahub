package com.linkedin.datahub.graphql.types.view;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.DataHubViewDefinition;
import com.linkedin.datahub.graphql.generated.DataHubViewFilter;
import com.linkedin.datahub.graphql.generated.DataHubViewType;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilter;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.LogicalOperator;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.view.DataHubViewInfo;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataHubViewMapper implements ModelMapper<EntityResponse, DataHubView> {

  private static final String KEYWORD_FILTER_SUFFIX = ".keyword";
  public static final DataHubViewMapper INSTANCE = new DataHubViewMapper();

  public static DataHubView map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataHubView apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataHubView result = new DataHubView();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATAHUB_VIEW);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<DataHubView> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(DATAHUB_VIEW_INFO_ASPECT_NAME, this::mapDataHubViewInfo);
    return mappingHelper.getResult();
  }

  private void mapDataHubViewInfo(@Nonnull final DataHubView view, @Nonnull final DataMap dataMap) {
    DataHubViewInfo viewInfo = new DataHubViewInfo(dataMap);
    view.setDescription(viewInfo.getDescription());
    view.setViewType(DataHubViewType.valueOf(viewInfo.getType().toString()));
    view.setName(viewInfo.getName());
    view.setDescription(viewInfo.getDescription());
    view.setDefinition(mapViewDefinition(viewInfo.getDefinition()));
  }

  @Nonnull
  private DataHubViewDefinition mapViewDefinition(
      @Nonnull final com.linkedin.view.DataHubViewDefinition definition) {
    final DataHubViewDefinition result = new DataHubViewDefinition();
    result.setFilter(mapFilter(definition.getFilter()));
    result.setEntityTypes(
        definition.getEntityTypes().stream()
            .map(EntityTypeMapper::getType)
            .collect(Collectors.toList()));
    return result;
  }

  @Nullable
  private DataHubViewFilter mapFilter(
      @Nonnull final com.linkedin.metadata.query.filter.Filter filter) {
    // This assumes that people DO NOT emit Views on their own, since we expect that the Filter
    // structure is within
    // a finite set of possibilities.
    //
    // If we find a View that was ingested manually and malformed, then we log that and return a
    // default.
    final DataHubViewFilter result = new DataHubViewFilter();
    if (filter.hasOr() && filter.getOr().size() == 1) {
      // Then we are looking at an AND with multiple sub conditions.
      result.setOperator(LogicalOperator.AND);
      result.setFilters(mapAndFilters(filter.getOr().get(0).getAnd()));
    } else {
      result.setOperator(LogicalOperator.OR);
      // Then we are looking at an OR with a group of sub conditions.
      result.setFilters(mapOrFilters(filter.getOr()));
    }
    return result;
  }

  /** This simply converts a List of leaf criterion into the FacetFiler equivalent. */
  @Nonnull
  private List<FacetFilter> mapAndFilters(@Nullable final List<Criterion> ands) {
    // If the array is missing, return empty array.
    if (ands == null) {
      log.warn("Found a View without any AND filter criteria. Returning empty filter list.");
      return Collections.emptyList();
    }
    return ands.stream().map(this::mapCriterion).collect(Collectors.toList());
  }

  /**
   * This converts a list of Conjunctive Criterion into a flattened list of FacetFilters. This
   * method makes the assumption that WE (our GraphQL API) has minted the View and that each or
   * criterion contains at maximum one nested condition.
   */
  @Nonnull
  private List<FacetFilter> mapOrFilters(@Nullable final List<ConjunctiveCriterion> ors) {
    if (ors == null) {
      log.warn("Found a View without any OR filter criteria. Returning empty filter list.");
      return Collections.emptyList();
    }
    if (ors.stream().anyMatch(or -> or.hasAnd() && or.getAnd().size() > 1)) {
      log.warn(
          String.format(
              "Detected a View with a malformed filter clause. OR view has children conjunctions with more than one Criterion. Returning empty filters. %s",
              ors));
      return Collections.emptyList();
    }
    // It is assumed that in this case, the view is a flat list of ORs. Thus, we filter
    // for all nested AND conditions containing only 1 entry.
    return ors.stream()
        .filter(or -> or.hasAnd() && or.getAnd().size() == 1)
        .map(or -> or.getAnd().get(0))
        .map(this::mapCriterion)
        .collect(Collectors.toList());
  }

  @Nonnull
  private FacetFilter mapCriterion(@Nonnull final Criterion andFilter) {
    final FacetFilter result = new FacetFilter();
    result.setField(stripKeyword(andFilter.getField()));
    result.setValues(andFilter.getValues());
    if (andFilter.hasCondition()) {
      result.setCondition(FilterOperator.valueOf(andFilter.getCondition().toString()));
    }
    if (andFilter.hasNegated()) {
      result.setNegated(andFilter.isNegated());
    }
    return result;
  }

  private String stripKeyword(final String fieldName) {
    // When the Filter is persisted, it may include a .keyword suffix.
    if (fieldName.endsWith(KEYWORD_FILTER_SUFFIX)) {
      return fieldName.substring(0, fieldName.length() - KEYWORD_FILTER_SUFFIX.length());
    }
    return fieldName;
  }
}
