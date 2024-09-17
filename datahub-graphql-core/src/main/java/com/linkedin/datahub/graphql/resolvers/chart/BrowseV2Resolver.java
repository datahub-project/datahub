package com.linkedin.datahub.graphql.resolvers.chart;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_V2_DELIMITER;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BrowseResultGroupV2;
import com.linkedin.datahub.graphql.generated.BrowseResultMetadata;
import com.linkedin.datahub.graphql.generated.BrowseResultsV2;
import com.linkedin.datahub.graphql.generated.BrowseV2Input;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BrowseV2Resolver implements DataFetcher<CompletableFuture<BrowseResultsV2>> {

  private final EntityClient _entityClient;
  private final ViewService _viewService;
  private final FormService _formService;

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  @Override
  public CompletableFuture<BrowseResultsV2> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final BrowseV2Input input = bindArgument(environment.getArgument("input"), BrowseV2Input.class);

    final List<String> entityNames = getEntityNames(input);
    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;
    final String query = input.getQuery() != null ? input.getQuery() : "*";
    final SearchFlags searchFlags = mapInputFlags(context, input.getSearchFlags());
    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(query);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final DataHubViewInfo maybeResolvedView =
                (input.getViewUrn() != null)
                    ? resolveView(
                        context.getOperationContext(),
                        _viewService,
                        UrnUtils.getUrn(input.getViewUrn()))
                    : null;
            final String pathStr =
                input.getPath().size() > 0
                    ? BROWSE_PATH_V2_DELIMITER
                        + String.join(BROWSE_PATH_V2_DELIMITER, input.getPath())
                    : "";
            final Filter inputFilter = ResolverUtils.buildFilter(null, input.getOrFilters());

            BrowseResultV2 browseResults =
                _entityClient.browseV2(
                    context.getOperationContext().withSearchFlags(flags -> searchFlags),
                    entityNames,
                    pathStr,
                    maybeResolvedView != null
                        ? SearchUtils.combineFilters(
                            inputFilter, maybeResolvedView.getDefinition().getFilter())
                        : inputFilter,
                    sanitizedQuery,
                    start,
                    count);
            return mapBrowseResults(context, browseResults);
          } catch (Exception e) {
            throw new RuntimeException("Failed to execute browse V2", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  public static List<String> getEntityNames(BrowseV2Input input) {
    List<EntityType> entityTypes;
    if (input.getTypes() != null && input.getTypes().size() > 0) {
      entityTypes = input.getTypes();
    } else if (input.getType() != null) {
      entityTypes = ImmutableList.of(input.getType());
    } else {
      entityTypes = BROWSE_ENTITY_TYPES;
    }
    return entityTypes.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());
  }

  private BrowseResultsV2 mapBrowseResults(
      @Nullable QueryContext context, BrowseResultV2 browseResults) {
    BrowseResultsV2 results = new BrowseResultsV2();
    results.setTotal(browseResults.getNumGroups());
    results.setStart(browseResults.getFrom());
    results.setCount(browseResults.getPageSize());

    List<BrowseResultGroupV2> groups = new ArrayList<>();
    browseResults
        .getGroups()
        .forEach(
            group -> {
              BrowseResultGroupV2 browseGroup = new BrowseResultGroupV2();
              browseGroup.setName(group.getName());
              browseGroup.setCount(group.getCount());
              browseGroup.setHasSubGroups(group.isHasSubGroups());
              if (group.hasUrn() && group.getUrn() != null) {
                browseGroup.setEntity(UrnToEntityMapper.map(context, group.getUrn()));
              }
              groups.add(browseGroup);
            });
    results.setGroups(groups);

    BrowseResultMetadata resultMetadata = new BrowseResultMetadata();
    resultMetadata.setPath(
        Arrays.stream(browseResults.getMetadata().getPath().split(BROWSE_PATH_V2_DELIMITER))
            .filter(pathComponent -> !"".equals(pathComponent))
            .collect(Collectors.toList()));
    resultMetadata.setTotalNumEntities(browseResults.getMetadata().getTotalNumEntities());
    results.setMetadata(resultMetadata);

    return results;
  }
}
