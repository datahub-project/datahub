package com.linkedin.datahub.graphql.resolvers.jobs;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.dataprocessinst.mappers.DataProcessInstanceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GraphQL Resolver used for fetching a list of Task Runs associated with a Data Job */
public class DataJobRunsResolver
    implements DataFetcher<CompletableFuture<DataProcessInstanceResult>> {

  private static final String PARENT_TEMPLATE_URN_SEARCH_INDEX_FIELD_NAME = "parentTemplate";
  private static final String CREATED_TIME_SEARCH_INDEX_FIELD_NAME = "created";
  private static final String HAS_RUN_EVENTS_FIELD_NAME = "hasRunEvents";
  private static final Logger log = LoggerFactory.getLogger(DataJobRunsResolver.class);

  private final EntityClient _entityClient;

  public DataJobRunsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<DataProcessInstanceResult> get(DataFetchingEnvironment environment) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();

          final String entityUrn = ((Entity) environment.getSource()).getUrn();
          final Integer start = environment.getArgumentOrDefault("start", 0);
          final Integer count = environment.getArgumentOrDefault("count", 20);

          try {
            // Step 1: Fetch set of task runs associated with the target entity from the Search
            // Index!
            // We use the search index so that we can easily sort by the last updated time.
            final Filter filter = buildTaskRunsEntityFilter(entityUrn);
            final List<SortCriterion> sortCriteria = buildTaskRunsSortCriteria();
            final SearchResult gmsResult =
                _entityClient.filter(
                    context.getOperationContext(),
                    Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME,
                    filter,
                    sortCriteria,
                    start,
                    count);
            final List<Urn> dataProcessInstanceUrns =
                gmsResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toList());

            // Step 2: Hydrate the incident entities
            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME,
                    new HashSet<>(dataProcessInstanceUrns),
                    null);

            // Step 3: Map GMS incident model to GraphQL model
            final List<EntityResponse> gmsResults = new ArrayList<>();
            for (Urn urn : dataProcessInstanceUrns) {
              gmsResults.add(entities.getOrDefault(urn, null));
            }
            final List<DataProcessInstance> dataProcessInstances =
                gmsResults.stream()
                    .filter(Objects::nonNull)
                    .map(p -> DataProcessInstanceMapper.map(context, p))
                    .collect(Collectors.toList());

            // Step 4: Package and return result
            final DataProcessInstanceResult result = new DataProcessInstanceResult();
            result.setCount(gmsResult.getPageSize());
            result.setStart(gmsResult.getFrom());
            result.setTotal(gmsResult.getNumEntities());
            result.setRuns(dataProcessInstances);
            return result;
          } catch (URISyntaxException | RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve incidents from GMS", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Filter buildTaskRunsEntityFilter(final String entityUrn) {
    CriterionArray array =
        new CriterionArray(
            ImmutableList.of(
                buildCriterion(
                    PARENT_TEMPLATE_URN_SEARCH_INDEX_FIELD_NAME, Condition.EQUAL, entityUrn),
                buildCriterion(
                    HAS_RUN_EVENTS_FIELD_NAME, Condition.EQUAL, Boolean.TRUE.toString())));

    final Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(ImmutableList.of(new ConjunctiveCriterion().setAnd(array))));
    return filter;
  }

  private List<SortCriterion> buildTaskRunsSortCriteria() {
    final SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField(CREATED_TIME_SEARCH_INDEX_FIELD_NAME);
    sortCriterion.setOrder(SortOrder.DESCENDING);
    return Collections.singletonList(sortCriterion);
  }
}
