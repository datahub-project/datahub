package com.linkedin.datahub.graphql.types.metric;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.Metric;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.metric.mappers.MetricMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;

public class MetricType implements SearchableEntityType<Metric, String> {

  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          Constants.METRIC_KEY_ASPECT_NAME,
          Constants.METRIC_INFO_ASPECT_NAME,
          Constants.METRIC_RELATIONSHIPS_ASPECT_NAME,
          Constants.METRIC_UPSTREAMS_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.DOMAINS_ASPECT_NAME,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
          Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
          Constants.STRUCTURED_PROPERTIES_ASPECT_NAME,
          Constants.STATUS_ASPECT_NAME,
          Constants.DEPRECATION_ASPECT_NAME,
          Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
          Constants.SUB_TYPES_ASPECT_NAME,
          Constants.DOCUMENTATION_ASPECT_NAME,
          Constants.BROWSE_PATHS_V2_ASPECT_NAME,
          Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME);

  private final EntityClient _entityClient;

  public MetricType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.METRIC;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Metric> objectClass() {
    return Metric.class;
  }

  @Override
  public List<DataFetcherResult<Metric>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> metricUrns = urns.stream().map(this::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.METRIC_ENTITY_NAME,
              metricUrns.stream()
                  .filter(urn -> canView(context.getOperationContext(), urn))
                  .collect(Collectors.toSet()),
              ASPECTS_TO_FETCH,
              true);

      final List<EntityResponse> gmsResults = new ArrayList<>(urns.size());
      for (Urn urn : metricUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<Metric>newResult()
                          .data(MetricMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Metrics", e);
    }
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      @Nullable Integer count,
      @Nonnull final QueryContext context)
      throws Exception {
    throw new NotImplementedException(
        "Searchable type (deprecated) not implemented on Metric entity type. Use searchAcrossEntities instead.");
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter filters,
      @Nullable Integer limit,
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            context.getOperationContext(), Constants.METRIC_ENTITY_NAME, query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  private Urn getUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to convert urn string %s into Urn", urnStr));
    }
  }
}
