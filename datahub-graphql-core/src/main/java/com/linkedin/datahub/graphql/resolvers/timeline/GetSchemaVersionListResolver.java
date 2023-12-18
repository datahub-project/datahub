package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.GetSchemaVersionListInput;
import com.linkedin.datahub.graphql.generated.GetSchemaVersionListResult;
import com.linkedin.datahub.graphql.types.timeline.mappers.SchemaVersionListMapper;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/*
Returns the most recent changes made to each column in a dataset at each dataset version.
 */
@Slf4j
public class GetSchemaVersionListResolver
    implements DataFetcher<CompletableFuture<GetSchemaVersionListResult>> {
  private final TimelineService _timelineService;

  public GetSchemaVersionListResolver(TimelineService timelineService) {
    _timelineService = timelineService;
  }

  @Override
  public CompletableFuture<GetSchemaVersionListResult> get(
      final DataFetchingEnvironment environment) throws Exception {
    final GetSchemaVersionListInput input =
        bindArgument(environment.getArgument("input"), GetSchemaVersionListInput.class);

    final String datasetUrnString = input.getDatasetUrn();
    final long startTime = 0;
    final long endTime = 0;

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final Set<ChangeCategory> changeCategorySet = new HashSet<>();
            changeCategorySet.add(ChangeCategory.TECHNICAL_SCHEMA);
            Urn datasetUrn = Urn.createFromString(datasetUrnString);
            List<ChangeTransaction> changeTransactionList =
                _timelineService.getTimeline(
                    datasetUrn, changeCategorySet, startTime, endTime, null, null, false);
            return SchemaVersionListMapper.map(changeTransactionList);
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
