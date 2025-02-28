package com.linkedin.datahub.graphql.resolvers.share;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.linkedin.common.Share;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ShareEntityResult;
import com.linkedin.datahub.graphql.generated.ShareLineageDirection;
import com.linkedin.datahub.graphql.generated.UnshareEntityInput;
import com.linkedin.datahub.graphql.types.common.mappers.ShareMapper;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.service.ShareService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.model.ExecuteUnshareResult;
import io.datahubproject.integrations.model.LineageDirection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UnshareEntityResolver implements DataFetcher<CompletableFuture<ShareEntityResult>> {

  private final ShareService _shareService;
  private final IntegrationsService _integrationsService;

  @Override
  public CompletableFuture<ShareEntityResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    final UnshareEntityInput input =
        bindArgument(environment.getArgument("input"), UnshareEntityInput.class);
    final Authentication authentication = context.getAuthentication();
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());
    if (!AuthorizationUtils.canShareEntity(entityUrn, context)) {
      throw new AuthorizationException(
          "Unauthorized to unshare this entity. Please contact your DataHub administrator.");
    }
    final ShareLineageDirection lineageDirection = input.getLineageDirection();
    LineageDirection shareLineageDirection;
    if (lineageDirection != null) {
      shareLineageDirection = LineageDirection.valueOf(lineageDirection.toString());
    } else {
      shareLineageDirection = null;
    }
    final List<Urn> connectionUrns =
        input.getConnectionUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    List<CompletableFuture<ExecuteUnshareResult>> unshareResultsList =
        connectionUrns.stream()
            .map(
                connectionUrn ->
                    _integrationsService.unshareEntity(
                        connectionUrn, entityUrn, shareLineageDirection))
            .collect(Collectors.toList());

    CompletableFuture<List<ExecuteUnshareResult>> unshareResultsFuture =
        GraphQLConcurrencyUtils.sequence(unshareResultsList);

    return unshareResultsFuture.thenCompose(
        unshareResults ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try {
                    // integrations service will update the share aspect of all entities if
                    // successful
                    boolean succeeded = true;
                    for (ExecuteUnshareResult unshareResult : unshareResults) {
                      if (unshareResult == null) {
                        succeeded = false;
                      }
                    }
                    Share shareAspect =
                        _shareService.getShareOrDefault(context.getOperationContext(), entityUrn);
                    ShareEntityResult shareEntityResult = new ShareEntityResult();
                    shareEntityResult.setSucceeded(succeeded);
                    shareEntityResult.setShare(ShareMapper.map(context, shareAspect));
                    return shareEntityResult;
                  } catch (Exception e) {
                    throw new RuntimeException(
                        String.format("Failed to unshare entity with input %s", input), e);
                  }
                },
                this.getClass().getSimpleName(),
                "get"));
  }
}
