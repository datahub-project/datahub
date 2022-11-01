package com.linkedin.datahub.graphql.resolvers.timeline;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.GetDatasetChangeEventsInput;
import com.linkedin.datahub.graphql.generated.GetDatasetChangeEventsResult;
import com.linkedin.datahub.graphql.types.timeline.mappers.DatasetChangeEventsMapper;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


/*
Returns the most recent changes made to each column in a dataset at each dataset version.
 */
@Slf4j
public class GetDatasetChangeEventsResolver implements DataFetcher<CompletableFuture<GetDatasetChangeEventsResult>> {
  private final TimelineService _timelineService;

  public GetDatasetChangeEventsResolver(TimelineService timelineService) {
    _timelineService = timelineService;
  }

  @Override
  public CompletableFuture<GetDatasetChangeEventsResult> get(final DataFetchingEnvironment environment) throws Exception {
    final GetDatasetChangeEventsInput input = bindArgument(environment.getArgument("input"), GetDatasetChangeEventsInput.class);

    final String datasetUrnString = input.getDatasetUrn();
    final long startTime = 0;
    final long endTime = 0;

    return CompletableFuture.supplyAsync(() -> {
      try {
        final Set<ChangeCategory> changeCategorySet = new HashSet<>();
        // include all change categories
        changeCategorySet.addAll(Arrays.asList(ChangeCategory.values()));
        Urn datasetUrn = Urn.createFromString(datasetUrnString);
        List<ChangeTransaction> changeTransactionList =
            _timelineService.getTimeline(datasetUrn, changeCategorySet, startTime, endTime, null, null, false);
        // return the latest changes first
        changeTransactionList.sort(Comparator.comparing(ChangeTransaction::getTimestamp).reversed());
        return DatasetChangeEventsMapper.map(changeTransactionList);

      } catch (URISyntaxException u) {
        log.error(
            String.format("Failed to list change events data, likely due to the Urn %s being invalid", datasetUrnString),
            u);
        return null;
      } catch (Exception e) {
        log.error("Failed to list change events data", e);
        return null;
      }
    });
  }
}