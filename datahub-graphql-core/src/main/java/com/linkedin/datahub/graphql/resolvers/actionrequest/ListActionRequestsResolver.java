package com.linkedin.datahub.graphql.resolvers.actionrequest;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActionRequestAssignee;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.AssigneeType;
import com.linkedin.datahub.graphql.generated.ListActionRequestsInput;
import com.linkedin.datahub.graphql.generated.ListActionRequestsResult;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils.*;
import static com.linkedin.metadata.Constants.*;


/**
 * Resolver responsible for resolving the 'listActionRequests' Query.
 */
public class ListActionRequestsResolver implements DataFetcher<CompletableFuture<ListActionRequestsResult>> {

  private static final String CREATED_FIELD_NAME = "created";
  private static final String ASSIGNED_USERS_FIELD_NAME = "assignedUsers";
  private static final String ASSIGNED_GROUPS_FIELD_NAME = "assignedGroups";

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final EntityClient _entityClient;

  public ListActionRequestsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListActionRequestsResult> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    final ListActionRequestsInput input = bindArgument(environment.getArgument("input"), ListActionRequestsInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final ActionRequestType type = input.getType() == null ? null : input.getType();
    final ActionRequestStatus status = input.getStatus() == null ? null : input.getStatus();
    final ActionRequestAssignee assignee = input.getAssignee() == null ? null : input.getAssignee();
    final Long startTimestampMillis = input.getStartTimestampMillis() == null ? null : input.getStartTimestampMillis();
    final Long endTimestampMillis = input.getEndTimestampMillis() == null ? null : input.getEndTimestampMillis();

    return CompletableFuture.supplyAsync(() -> {
      try {

        Urn actorUrn = null;
        List<Urn> groupUrns = null;

        if (assignee == null) {
          // Case 1: If no assignee filter provided, fall back to filtering for current user and their groups.
          actorUrn = Urn.createFromString(context.getActorUrn());
          final Optional<GroupMembership> maybeGroupMembership =
              resolveGroupMembership(actorUrn, context.getAuthentication(), _entityClient);
          groupUrns = maybeGroupMembership.<List<Urn>>map(GroupMembership::getGroups).orElse(null);
        } else {
          // Case 2: Caller provided a user or group assignee filter.
          final Urn assigneeUrn = Urn.createFromString(assignee.getUrn());
          if (AssigneeType.GROUP.equals(assignee.getType())) {
            groupUrns = Collections.singletonList(assigneeUrn);
          } else {
            actorUrn = assigneeUrn;
          }
        }

        final Filter filter = createFilter(actorUrn, groupUrns, type, status, startTimestampMillis, endTimestampMillis);

        final SortCriterion sortCriterion = new SortCriterion()
            .setField(CREATED_FIELD_NAME)
            .setOrder(SortOrder.DESCENDING);

        final SearchResult searchResult = _entityClient.filter(
            ACTION_REQUEST_ENTITY_NAME,
            filter,
            sortCriterion,
            start,
            count,
            context.getAuthentication());

        final Map<Urn, Entity> entities = _entityClient.batchGet(new HashSet<>(searchResult.getEntities().stream().map(result -> result.getEntity()).collect(
            Collectors.toList())), context.getAuthentication());

        final ListActionRequestsResult result = new ListActionRequestsResult();
        result.setStart(searchResult.getFrom());
        result.setCount(searchResult.getPageSize());
        result.setTotal(searchResult.getNumEntities());
        result.setActionRequests(ActionRequestUtils.mapActionRequests(entities.values()));
        return result;
      } catch (Exception e) {
        throw new RuntimeException("Failed to list action requests", e);
      }
    });
  }

  private Filter createFilter(final @Nullable Urn actorUrn, final @Nullable List<Urn> groupUrns,
      final @Nullable ActionRequestType type, final @Nullable ActionRequestStatus status,
      final @Nullable Long startTimestampMillis, final @Nullable Long endTimestampMillis) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    // If actor and group are both provided, "or" the results.
    if (actorUrn != null) {
      disjunction.add(createUserFilterConjunction(actorUrn, type, status, startTimestampMillis, endTimestampMillis));
    }
    if (groupUrns != null) {
      disjunction.addAll(
          createGroupFilterDisjunction(groupUrns, type, status, startTimestampMillis, endTimestampMillis));
    }
    filter.setOr(disjunction);
    return filter;
  }

  private ConjunctiveCriterion createUserFilterConjunction(final Urn userUrn, final @Nullable ActionRequestType type,
      final @Nullable ActionRequestStatus status, final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();
    final Criterion userCriterion = new Criterion();
    userCriterion.setField(ASSIGNED_USERS_FIELD_NAME + ".keyword");
    userCriterion.setValue(userUrn.toString());
    userCriterion.setCondition(Condition.EQUAL);
    andCriterion.add(userCriterion);
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
    return conjunction;
  }

  private List<ConjunctiveCriterion> createGroupFilterDisjunction(final List<Urn> groupUrns,
      final @Nullable ActionRequestType type, final @Nullable ActionRequestStatus status,
      final @Nullable Long startTimestampMillis, final @Nullable Long endTimestampMillis) {
    final List<ConjunctiveCriterion> disjunction = new ArrayList<>();
    // Create a new filter for each group urn, where the urn, type, and status must all match.
    for (Urn groupUrn : groupUrns) {
      final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      final CriterionArray andCriterion = new CriterionArray();
      final Criterion groupUrnCriterion = new Criterion();
      groupUrnCriterion.setField(ASSIGNED_GROUPS_FIELD_NAME + ".keyword");
      groupUrnCriterion.setValue(groupUrn.toString());
      groupUrnCriterion.setCondition(Condition.EQUAL);
      andCriterion.add(groupUrnCriterion);
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
