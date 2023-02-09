package com.linkedin.datahub.graphql.resolvers.jobs;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.DataProcessInstanceFilterInput;
import com.linkedin.datahub.graphql.generated.DataProcessInstanceInput;
import com.linkedin.datahub.graphql.generated.DataProcessInstanceResult;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.RelationshipDirection;
import com.linkedin.datahub.graphql.types.dataprocessinst.mappers.DataProcessInstanceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


/**
 * GraphQL Resolver used for fetching the list of task runs associated with a Dataset.
 */
public class EntityRunsResolver implements DataFetcher<CompletableFuture<DataProcessInstanceResult>> {

  private static final String INPUT_FIELD_NAME = "inputs.keyword";
  private static final String OUTPUT_FIELD_NAME = "outputs.keyword";
  private static final String CREATED_TIME_SEARCH_INDEX_FIELD_NAME = "created";
  private static final String LOGICAL_DATE_SEARCH_INDEX_FIELD_NAME = "logicalDate";
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final List<DataProcessInstanceFilterInput> DEFAULT_FILTER = null;
  private static final DataProcessInstanceInput DEFAULT_DATA_PROCESS_INSTANCE_INPUT = new DataProcessInstanceInput();

  private final EntityClient _entityClient;

  public EntityRunsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<DataProcessInstanceResult> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(() -> {

      final QueryContext context = environment.getContext();

      final String entityUrn = ((Entity) environment.getSource()).getUrn();
      final RelationshipDirection direction = RelationshipDirection.valueOf(environment.getArgumentOrDefault("direction",
          RelationshipDirection.INCOMING.toString()));
      final DataProcessInstanceInput input = bindArgument(
          environment.getArgumentOrDefault("input", DEFAULT_DATA_PROCESS_INSTANCE_INPUT),
          DataProcessInstanceInput.class);

      final Integer start = input.getStart() != null ? input.getStart() : DEFAULT_START;
      final Integer count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;
      final List<DataProcessInstanceFilterInput> filters = input.getFilters() != null ? input.getFilters()
          : DEFAULT_FILTER;

      try {
        // Step 1: Fetch set of task runs associated with the target entity from the Search Index!
        // We use the search index so that we can easily sort by the last updated time.
        final Filter filter = buildTaskRunsEntityFilter(entityUrn, filters, direction);
        final SortCriterion sortCriterion = buildTaskRunsSortCriterion();
        final SearchResult gmsResult = _entityClient.filter(
            Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME,
            filter,
            sortCriterion,
            start,
            count,
            context.getAuthentication());
        final List<Urn> dataProcessInstanceUrns = gmsResult.getEntities()
            .stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toList());

        // Step 2: Hydrate the incident entities
        final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(
            Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME,
            new HashSet<>(dataProcessInstanceUrns),
            null,
            context.getAuthentication());

        // Step 3: Map GMS instance model to GraphQL model
        final List<EntityResponse> gmsResults = new ArrayList<>();
        for (Urn urn : dataProcessInstanceUrns) {
          gmsResults.add(entities.getOrDefault(urn, null));
        }
        final List<DataProcessInstance> dataProcessInstances = gmsResults.stream()
            .filter(Objects::nonNull)
            .map(DataProcessInstanceMapper::map)
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
    });
  }

  private Filter buildTaskRunsEntityFilter(final String entityUrn, @Nullable final List<DataProcessInstanceFilterInput> filters, final RelationshipDirection direction) {
    ImmutableList.Builder<Criterion> criterionBuilder = ImmutableList.builder();
    Criterion directionCriterion = new Criterion()
      .setField(direction.equals(RelationshipDirection.INCOMING) ? INPUT_FIELD_NAME : OUTPUT_FIELD_NAME)
      .setCondition(Condition.EQUAL)
      .setValue(entityUrn);
    criterionBuilder.add(directionCriterion);
    if (filters != null) {
      List<Criterion> userDefinedFilters = filters.stream().map(f -> {
        switch (f.getType()) {
          case BEFORE_LOGICAL_DATE:
            return new Criterion().setField(LOGICAL_DATE_SEARCH_INDEX_FIELD_NAME)
                .setCondition(Condition.LESS_THAN_OR_EQUAL_TO).setValue(f.getValue());
          case ON_LOGICAL_DATE:
            return new Criterion().setField(LOGICAL_DATE_SEARCH_INDEX_FIELD_NAME).setCondition(Condition.EQUAL)
                .setValue(f.getValue());
          case AFTER_LOGICAL_DATE:
            return new Criterion().setField(LOGICAL_DATE_SEARCH_INDEX_FIELD_NAME)
                .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO).setValue(f.getValue());
          default:
            return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList());
      criterionBuilder.addAll(userDefinedFilters);
    }
    CriterionArray array = new CriterionArray(criterionBuilder.build());
    final Filter filter = new Filter();
    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(
        new ConjunctiveCriterion()
            .setAnd(array)
    )));
    return filter;
  }

  private SortCriterion buildTaskRunsSortCriterion() {
    final SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField(CREATED_TIME_SEARCH_INDEX_FIELD_NAME);
    sortCriterion.setOrder(SortOrder.DESCENDING);
    return sortCriterion;
  }
}
