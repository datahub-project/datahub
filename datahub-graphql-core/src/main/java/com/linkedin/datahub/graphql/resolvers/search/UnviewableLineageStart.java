package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchPath;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Lineage searches started from an entity the caller cannot view answer as if the entity did not
 * exist. The results built here mirror what {@link LineageSearchService} returns for a start urn
 * with no lineage, so the two cases cannot be told apart.
 */
public final class UnviewableLineageStart {

  private UnviewableLineageStart() {}

  public static boolean isUnviewable(@Nonnull final QueryContext context, @Nonnull final Urn urn) {
    return !AuthorizationUtils.canView(context.getOperationContext(), urn);
  }

  @Nonnull
  public static LineageSearchResult emptySearchResult(
      @Nonnull final QueryContext context, final int start, final int count) {
    return new LineageSearchResult()
        .setEntities(new LineageSearchEntityArray(Collections.emptyList()))
        .setMetadata(new SearchResultMetadata().setAggregations(degreeFacetOnly()))
        .setFrom(start)
        .setPageSize(ConfigUtils.applyLimit(context.getDataHubAppConfig().getGraphService(), count))
        .setNumEntities(0)
        .setLineageSearchPath(LineageSearchPath.TORTOISE)
        .setIsPartial(false);
  }

  @Nonnull
  public static LineageScrollResult emptyScrollResult(
      @Nonnull final QueryContext context, final int count) {
    return new LineageScrollResult()
        .setEntities(new LineageSearchEntityArray(Collections.emptyList()))
        .setMetadata(new SearchResultMetadata().setAggregations(degreeFacetOnly()))
        .setPageSize(ConfigUtils.applyLimit(context.getDataHubAppConfig().getGraphService(), count))
        .setNumEntities(0)
        .setIsPartial(false);
  }

  private static AggregationMetadataArray degreeFacetOnly() {
    return new AggregationMetadataArray(
        Collections.singletonList(LineageSearchService.DEGREE_FILTER_GROUP));
  }
}
