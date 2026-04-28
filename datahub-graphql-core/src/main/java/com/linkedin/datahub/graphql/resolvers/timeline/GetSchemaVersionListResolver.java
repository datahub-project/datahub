package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetSchemaVersionListInput;
import com.linkedin.datahub.graphql.generated.GetSchemaVersionListResult;
import com.linkedin.datahub.graphql.types.timeline.mappers.SchemaVersionListMapper;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
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
    final QueryContext context = environment.getContext();
    final GetSchemaVersionListInput input =
        bindArgument(environment.getArgument("input"), GetSchemaVersionListInput.class);

    final String datasetUrnString = input.getDatasetUrn();
    final long startTime = 0;
    final long endTime = 0;

    // Parse URN and enforce view authorization synchronously so errors propagate
    // rather than being swallowed by the generic catch inside supplyAsync.
    final Urn datasetUrn = UrnUtils.getUrn(datasetUrnString);
    if (!AuthUtil.canViewEntity(context.getOperationContext(), datasetUrn)) {
      throw new AuthorizationException(
          "Unauthorized to view schema version list for entity: " + datasetUrn);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Set<ChangeCategory> changeCategorySet = new HashSet<>();
            changeCategorySet.add(ChangeCategory.TECHNICAL_SCHEMA);
            List<ChangeTransaction> changeTransactionList =
                _timelineService.getTimeline(
                    datasetUrn, changeCategorySet, startTime, endTime, null, null, false);
            return SchemaVersionListMapper.map(changeTransactionList);
          } catch (Exception e) {
            log.error("Failed to list schema version data", e);
            return null;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
