package com.linkedin.datahub.graphql.resolvers.actionrequest;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getEntityNames;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
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
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Resolver responsible for resolving the 'listActionRequests' Query. */
public class ListActionRequestsResolver
    implements DataFetcher<CompletableFuture<ListActionRequestsResult>> {

  private static final String CREATED_FIELD_NAME = "created";
  private static final String ASSIGNED_USERS_FIELD_NAME = "assignedUsers";
  private static final String ASSIGNED_GROUPS_FIELD_NAME = "assignedGroups";
  private static final String ASSIGNED_ROLES_FIELD_NAME = "assignedRoles";

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final EntityClient _entityClient;

  public ListActionRequestsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListActionRequestsResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    final ListActionRequestsInput input =
        bindArgument(environment.getArgument("input"), ListActionRequestsInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final ActionRequestType type = input.getType() == null ? null : input.getType();
    final ActionRequestStatus status = input.getStatus() == null ? null : input.getStatus();
    final ActionRequestAssignee assignee = input.getAssignee() == null ? null : input.getAssignee();
    final boolean getAllActionRequests =
        input.getAllActionRequests() == null ? false : input.getAllActionRequests();
    final Urn resourceUrn =
        input.getResourceUrn() == null ? null : UrnUtils.getUrn(input.getResourceUrn());
    final Long startTimestampMillis =
        input.getStartTimestampMillis() == null ? null : input.getStartTimestampMillis();
    final Long endTimestampMillis =
        input.getEndTimestampMillis() == null ? null : input.getEndTimestampMillis();
    final List<String> facets = input.getFacets() == null ? null : input.getFacets();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            Urn actorUrn = null;
            List<Urn> groupUrns = null;
            List<Urn> roleUrns = null;

            if (assignee == null) {
              // Case 1: If no assignee filter provided, fall back to filtering for current user and
              // their groups.
              if (!getAllActionRequests) {
                actorUrn = Urn.createFromString(context.getActorUrn());
                AssignedUrns groupAndRoleUrns =
                    getGroupAndRoleUrns(context.getOperationContext(), actorUrn, _entityClient);
                groupUrns = groupAndRoleUrns.getGroupUrns();
                roleUrns = groupAndRoleUrns.getRoleUrns();
              }
            } else {
              // Case 2: Caller provided a user or group assignee filter.
              final Urn assigneeUrn = Urn.createFromString(assignee.getUrn());
              if (AssigneeType.GROUP.equals(assignee.getType())) {
                // We do not compute role urns from group urns because only users are assigned to
                // roles.
                groupUrns = Collections.singletonList(assigneeUrn);
              } else {
                actorUrn = assigneeUrn;
                AssignedUrns groupAndRoleUrns =
                    getGroupAndRoleUrns(context.getOperationContext(), actorUrn, _entityClient);
                groupUrns = groupAndRoleUrns.getGroupUrns();
                roleUrns = groupAndRoleUrns.getRoleUrns();
              }
            }

            final Filter baseFilter =
                createFilter(
                    actorUrn,
                    groupUrns,
                    roleUrns,
                    type,
                    status,
                    resourceUrn,
                    startTimestampMillis,
                    endTimestampMillis);

            final Filter filter;
            if (input.getOrFilters() != null && !input.getOrFilters().isEmpty()) {
              filter =
                  FilterUtils.combineFilters(
                      baseFilter, ResolverUtils.buildFilter(null, input.getOrFilters()));
            } else {
              filter = baseFilter;
            }

            final List<SortCriterion> sortCriteria =
                Collections.singletonList(
                    new SortCriterion()
                        .setField(CREATED_FIELD_NAME)
                        .setOrder(SortOrder.DESCENDING));

            final com.linkedin.metadata.query.SearchFlags searchFlags =
                new com.linkedin.metadata.query.SearchFlags();
            searchFlags.setSkipCache(true);
            searchFlags.setSkipHighlighting(true);

            final SearchResult searchResult =
                _entityClient.searchAcrossEntities(
                    context.getOperationContext().withSearchFlags(flags -> searchFlags),
                    getEntityNames(ImmutableList.of(EntityType.ACTION_REQUEST)),
                    "*",
                    filter,
                    start,
                    count,
                    sortCriteria,
                    facets,
                    null);

            final List<Urn> entityUrns =
                searchResult.getEntities().stream().map(SearchEntity::getEntity).toList();

            final Map<Urn, Entity> entityMap =
                _entityClient.batchGet(context.getOperationContext(), new HashSet<>(entityUrns));

            final List<Entity> entities = new ArrayList<>();
            entityUrns.forEach((urn) -> entities.add(entityMap.get(urn)));

            final ListActionRequestsResult result = new ListActionRequestsResult();
            result.setStart(searchResult.getFrom());
            result.setCount(searchResult.getPageSize());
            result.setTotal(searchResult.getNumEntities());
            result.setActionRequests(ActionRequestUtils.mapActionRequests(context, entities));
            result.setFacets(
                searchResult.getMetadata().getAggregations().stream()
                    .map(f -> MapperUtils.mapFacet(context, f))
                    .collect(Collectors.toList()));

            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list action requests", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  Filter createFilter(
      final @Nullable Urn actorUrn,
      final @Nullable List<Urn> groupUrns,
      final @Nullable List<Urn> roleUrns,
      final @Nullable ActionRequestType type,
      final @Nullable ActionRequestStatus status,
      final @Nullable Urn resourceUrn,
      final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();

    if (hasAssigneeFilters(actorUrn, groupUrns, roleUrns)) {
      addAssigneeFilters(
          disjunction,
          actorUrn,
          groupUrns,
          roleUrns,
          type,
          status,
          resourceUrn,
          startTimestampMillis,
          endTimestampMillis);
    } else {
      addNonAssigneeFilters(
          disjunction, type, status, resourceUrn, startTimestampMillis, endTimestampMillis);
    }

    filter.setOr(disjunction);
    return filter;
  }

  private boolean hasAssigneeFilters(Urn actorUrn, List<Urn> groupUrns, List<Urn> roleUrns) {
    return actorUrn != null
        || (groupUrns != null && !groupUrns.isEmpty())
        || (roleUrns != null && !roleUrns.isEmpty());
  }

  private void addAssigneeFilters(
      ConjunctiveCriterionArray disjunction,
      Urn actorUrn,
      List<Urn> groupUrns,
      List<Urn> roleUrns,
      ActionRequestType type,
      ActionRequestStatus status,
      Urn resourceUrn,
      Long startTimestampMillis,
      Long endTimestampMillis) {
    if (actorUrn != null) {
      disjunction.add(
          createUserFilterConjunction(
              actorUrn, type, status, resourceUrn, startTimestampMillis, endTimestampMillis));
    }
    if (groupUrns != null) {
      disjunction.addAll(
          createGroupFilterDisjunction(
              groupUrns, type, status, resourceUrn, startTimestampMillis, endTimestampMillis));
    }
    if (roleUrns != null) {
      disjunction.addAll(
          createRoleFilterDisjunction(
              roleUrns, type, status, startTimestampMillis, endTimestampMillis));
    }
  }

  private void addNonAssigneeFilters(
      ConjunctiveCriterionArray disjunction,
      ActionRequestType type,
      ActionRequestStatus status,
      Urn resourceUrn,
      Long startTimestampMillis,
      Long endTimestampMillis) {
    if (hasNonAssigneeFilters(
        type, status, resourceUrn, startTimestampMillis, endTimestampMillis)) {
      final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      final CriterionArray andCriterion = new CriterionArray();

      if (status != null) {
        andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
      }
      if (type != null) {
        andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
      }
      if (resourceUrn != null) {
        andCriterion.add(ActionRequestUtils.createResourceCriterion(resourceUrn.toString()));
      }
      if (startTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createStartTimestampCriterion(startTimestampMillis));
      }
      if (endTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createEndTimestampCriterion(endTimestampMillis));
      }

      conjunction.setAnd(andCriterion);
      disjunction.add(conjunction);
    }
  }

  private boolean hasNonAssigneeFilters(
      ActionRequestType type,
      ActionRequestStatus status,
      Urn resourceUrn,
      Long startTimestampMillis,
      Long endTimestampMillis) {
    return status != null
        || type != null
        || resourceUrn != null
        || startTimestampMillis != null
        || endTimestampMillis != null;
  }

  private <T> void addCriterionIfNotNull(
      CriterionArray criteria,
      T value,
      java.util.function.Function<T, Criterion> criterionCreator) {
    if (value != null) {
      criteria.add(criterionCreator.apply(value));
    }
  }

  private ConjunctiveCriterion createUserFilterConjunction(
      final Urn userUrn,
      final @Nullable ActionRequestType type,
      final @Nullable ActionRequestStatus status,
      final @Nullable Urn resourceUrn,
      final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();
    andCriterion.add(
        CriterionUtils.buildCriterion(
            ASSIGNED_USERS_FIELD_NAME + ".keyword", Condition.EQUAL, userUrn.toString()));
    if (status != null) {
      andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
    }
    if (type != null) {
      andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
    }
    if (resourceUrn != null) {
      andCriterion.add(ActionRequestUtils.createResourceCriterion(resourceUrn.toString()));
    }
    if (startTimestampMillis != null) {
      andCriterion.add(ActionRequestUtils.createStartTimestampCriterion(startTimestampMillis));
    }
    if (endTimestampMillis != null) {
      andCriterion.add(ActionRequestUtils.createEndTimestampCriterion(endTimestampMillis));
    }
    conjunction.setAnd(andCriterion);
    return conjunction;
  }

  private List<ConjunctiveCriterion> createGroupFilterDisjunction(
      final List<Urn> groupUrns,
      final @Nullable ActionRequestType type,
      final @Nullable ActionRequestStatus status,
      final @Nullable Urn resourceUrn,
      final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final List<ConjunctiveCriterion> disjunction = new ArrayList<>();
    // Create a new filter for each group urn, where the urn, type, and status must all match.
    for (Urn groupUrn : groupUrns) {
      final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      final CriterionArray andCriterion = new CriterionArray();
      andCriterion.add(
          CriterionUtils.buildCriterion(
              ASSIGNED_GROUPS_FIELD_NAME + ".keyword", Condition.EQUAL, groupUrn.toString()));
      if (status != null) {
        andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
      }
      if (type != null) {
        andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
      }
      if (resourceUrn != null) {
        andCriterion.add(ActionRequestUtils.createResourceCriterion(resourceUrn.toString()));
      }
      if (startTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createStartTimestampCriterion(startTimestampMillis));
      }
      if (endTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createEndTimestampCriterion(endTimestampMillis));
      }
      conjunction.setAnd(andCriterion);
      disjunction.add(conjunction);
    }
    return disjunction;
  }

  private List<ConjunctiveCriterion> createRoleFilterDisjunction(
      final List<Urn> roleUrns,
      final @Nullable ActionRequestType type,
      final @Nullable ActionRequestStatus status,
      final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final List<ConjunctiveCriterion> disjunction = new ArrayList<>();
    // Create a new filter for each role urn, where the urn, type, and status must all match.
    for (Urn groupUrn : roleUrns) {
      final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      final CriterionArray andCriterion = new CriterionArray();
      andCriterion.add(
          CriterionUtils.buildCriterion(
              ASSIGNED_ROLES_FIELD_NAME + ".keyword", Condition.EQUAL, groupUrn.toString()));
      if (status != null) {
        andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
      }
      if (type != null) {
        andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
      }
      if (startTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createStartTimestampCriterion(startTimestampMillis));
      }
      if (endTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createEndTimestampCriterion(endTimestampMillis));
      }
      conjunction.setAnd(andCriterion);
      disjunction.add(conjunction);
    }
    return disjunction;
  }
}
