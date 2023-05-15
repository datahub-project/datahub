package com.linkedin.datahub.graphql.resolvers.chart;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BrowseInput;
import com.linkedin.datahub.graphql.generated.BrowseInputV2;
import com.linkedin.datahub.graphql.generated.BrowseResultGroup;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.BrowseResultsV2;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_V2_DELIMITER;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getEntityNames;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.resolveView;

@Slf4j
@RequiredArgsConstructor
public class BrowseV2Resolver implements DataFetcher<CompletableFuture<BrowseResults>> {

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  @Override
  public CompletableFuture<BrowseResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final BrowseInputV2 input = bindArgument(environment.getArgument("input"), BrowseInputV2.class);
    final String entityName = EntityTypeMapper.getName(input.getType());

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;
    final String query = input.getQuery() != null ? input.getQuery() : "*";

    return CompletableFuture.supplyAsync(() -> {
      try {
        final DataHubViewInfo maybeResolvedView = (input.getViewUrn() != null)
            ? resolveView(_viewService, UrnUtils.getUrn(input.getViewUrn()), context.getAuthentication())
            : null;
        final String pathStr = input.getPath().size() > 0 ? BROWSE_PATH_V2_DELIMITER + String.join(BROWSE_PATH_V2_DELIMITER, input.getPath()) : "";
        final Filter filter = ResolverUtils.buildFilter(null, input.getOrFilters());

        BrowseResult browseResults = _entityClient.browseV2(
            entityName,
            pathStr,
            maybeResolvedView != null
            ? SearchUtils.combineFilters(filter, maybeResolvedView.getDefinition().getFilter())
            : filter,
            query,
            start,
            count,
            environment.getLocalContext()
        );

        BrowseResults results = new BrowseResults();
        List<BrowseResultGroup> groups = new ArrayList<>();
        browseResults.getGroups().forEach(group -> {
          BrowseResultGroup browseGroup = new BrowseResultGroup();
          browseGroup.setName(group.getName());
          browseGroup.setCount(group.getCount());
          groups.add(browseGroup);
        });
        results.setGroups(groups);

        return results;
      } catch (Exception e) {
        throw new RuntimeException("Failed to execute browse V2", e);
      }
    });
  }
}

