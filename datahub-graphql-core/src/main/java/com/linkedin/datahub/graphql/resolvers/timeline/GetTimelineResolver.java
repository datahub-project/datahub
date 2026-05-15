package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.GetTimelineInput;
import com.linkedin.datahub.graphql.generated.GetTimelineResult;
import com.linkedin.datahub.graphql.types.timeline.mappers.ChangeTransactionMapper;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/*
Returns the timeline in its original form
 */
@Slf4j
public class GetTimelineResolver implements DataFetcher<CompletableFuture<GetTimelineResult>> {
  private final TimelineService _timelineService;

  public GetTimelineResolver(TimelineService timelineService) {
    _timelineService = timelineService;
  }

  @Override
  public CompletableFuture<GetTimelineResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final GetTimelineInput input =
        bindArgument(environment.getArgument("input"), GetTimelineInput.class);

    final String entityUrnString = input.getUrn();
    final List<ChangeCategoryType> changeCategories = input.getChangeCategories();

    // Parse URN and enforce view authorization synchronously so errors propagate
    // rather than being swallowed by the generic catch inside supplyAsync.
    final Urn entityUrn = UrnUtils.getUrn(entityUrnString);
    if (!AuthUtil.canViewEntity(context.getOperationContext(), entityUrn)) {
      throw new AuthorizationException(
          "Unauthorized to view change history for entity: " + entityUrn);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Set<ChangeCategory> changeCategorySet =
                changeCategories != null
                    ? changeCategories.stream()
                        .map(
                            changeCategoryType ->
                                ChangeCategory.valueOf(changeCategoryType.toString()))
                        .collect(Collectors.toSet())
                    : Arrays.stream(ChangeCategory.values()).collect(Collectors.toSet());
            final List<ChangeTransaction> changeTransactionList =
                _timelineService.getTimeline(
                    entityUrn,
                    changeCategorySet,
                    TimelineService.DEFAULT_MAX_CHANGE_TRANSACTIONS,
                    false);
            GetTimelineResult result = new GetTimelineResult();
            result.setChangeTransactions(
                changeTransactionList.stream()
                    .map(ChangeTransactionMapper::map)
                    .filter(t -> t.getChanges() != null && !t.getChanges().isEmpty())
                    .collect(Collectors.toList()));
            return result;
          } catch (Exception e) {
            log.error(
                String.format("Failed to get timeline data for entity %s", entityUrnString), e);
            return null;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
