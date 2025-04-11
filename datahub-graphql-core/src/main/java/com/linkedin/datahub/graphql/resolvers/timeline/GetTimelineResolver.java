package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.GetTimelineInput;
import com.linkedin.datahub.graphql.generated.GetTimelineResult;
import com.linkedin.datahub.graphql.types.timeline.mappers.ChangeTransactionMapper;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
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
    final GetTimelineInput input =
        bindArgument(environment.getArgument("input"), GetTimelineInput.class);

    final String datasetUrnString = input.getUrn();
    final List<ChangeCategoryType> changeCategories = input.getChangeCategories();
    final long startTime = 0;
    final long endTime = 0;
    //    final String version = input.getVersion() == null ? null : input.getVersion();

    return CompletableFuture.supplyAsync(
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
            final Urn datasetUrn = Urn.createFromString(datasetUrnString);
            final List<ChangeTransaction> changeTransactionList =
                _timelineService.getTimeline(
                    datasetUrn, changeCategorySet, startTime, endTime, null, null, false);
            GetTimelineResult result = new GetTimelineResult();
            result.setChangeTransactions(
                changeTransactionList.stream()
                    .map(ChangeTransactionMapper::map)
                    .collect(Collectors.toList()));
            return result;
          } catch (URISyntaxException u) {
            log.error(
                String.format(
                    "Failed to list schema blame data, likely due to the Urn %s being invalid",
                    datasetUrnString),
                u);
            return null;
          } catch (Exception e) {
            log.error("Failed to list schema blame data", e);
            return null;
          }
        });
  }
}
