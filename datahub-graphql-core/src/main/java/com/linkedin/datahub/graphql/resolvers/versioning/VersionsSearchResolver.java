package com.linkedin.datahub.graphql.resolvers.versioning;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.CriterionUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver that executes a searchAcrossEntities only on a version set's versioned entities */
@Slf4j
@RequiredArgsConstructor
public class VersionsSearchResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final String VERSION_SET_FIELD_NAME = "versionSet";

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  @Override
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final Entity entity = environment.getSource();
    final QueryContext context = environment.getContext();
    final SearchAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossEntitiesInput.class);

    final Criterion versionSetFilter =
        CriterionUtils.buildCriterion(VERSION_SET_FIELD_NAME, Condition.EQUAL, entity.getUrn());
    final Filter baseFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(versionSetFilter))));
    final Filter inputFilter = ResolverUtils.buildFilter(null, input.getOrFilters());

    final List<SortCriterion> initialSortCriteria =
        SearchUtils.getSortCriteria(input.getSortInput());
    final List<SortCriterion> sortCriteria =
        Stream.concat(
                initialSortCriteria.stream(),
                Stream.of(
                    new SortCriterion()
                        .setField(VERSION_SORT_ID_FIELD_NAME)
                        .setOrder(SortOrder.DESCENDING)))
            .toList();

    SearchFlags searchFlags = Optional.ofNullable(input.getSearchFlags()).orElse(new SearchFlags());
    searchFlags.setFilterNonLatestVersions(false);

    return SearchUtils.searchAcrossEntities(
        context,
        _entityClient,
        _viewService,
        input.getTypes(),
        input.getQuery(),
        SearchUtils.combineFilters(inputFilter, baseFilter),
        input.getViewUrn(),
        sortCriteria,
        searchFlags,
        input.getCount(),
        input.getStart(),
        this.getClass().getSimpleName());
  }
}
