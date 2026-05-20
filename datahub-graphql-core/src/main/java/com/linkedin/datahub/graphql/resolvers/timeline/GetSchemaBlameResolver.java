package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetSchemaBlameInput;
import com.linkedin.datahub.graphql.generated.GetSchemaBlameResult;
import com.linkedin.datahub.graphql.types.timeline.mappers.SchemaBlameMapper;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/*
Returns the most recent changes made to each column in a dataset at each dataset version.
 */
@Slf4j
public class GetSchemaBlameResolver
    implements DataFetcher<CompletableFuture<GetSchemaBlameResult>> {
  private final TimelineService _timelineService;

  public GetSchemaBlameResolver(TimelineService timelineService) {
    _timelineService = timelineService;
  }

  @Override
  public CompletableFuture<GetSchemaBlameResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final GetSchemaBlameInput input =
        bindArgument(environment.getArgument("input"), GetSchemaBlameInput.class);

    final String datasetUrnString = input.getDatasetUrn();
    final long startTime = 0;
    final long endTime = 0;
    final String version = input.getVersion() == null ? null : input.getVersion();

    // Parse URN and enforce view authorization synchronously so errors propagate
    // rather than being swallowed by the generic catch inside supplyAsync.
    final Urn datasetUrn = UrnUtils.getUrn(datasetUrnString);
    if (!AuthUtil.canViewEntity(context.getOperationContext(), datasetUrn)) {
      throw new AuthorizationException(
          "Unauthorized to view schema blame for entity: " + datasetUrn);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Set<ChangeCategory> changeCategorySet =
                Collections.singleton(ChangeCategory.TECHNICAL_SCHEMA);
            final List<ChangeTransaction> changeTransactionList =
                _timelineService.getTimeline(
                    datasetUrn, changeCategorySet, startTime, endTime, null, null, false);
            return SchemaBlameMapper.map(changeTransactionList, version);
          } catch (Exception e) {
            log.error("Failed to list schema blame data", e);
            return null;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
