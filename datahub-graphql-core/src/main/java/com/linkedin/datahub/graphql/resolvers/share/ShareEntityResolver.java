package com.linkedin.datahub.graphql.resolvers.share;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.Share;
import com.linkedin.common.ShareResultState;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ShareEntityInput;
import com.linkedin.datahub.graphql.generated.ShareEntityResult;
import com.linkedin.datahub.graphql.generated.ShareLineageDirection;
import com.linkedin.datahub.graphql.types.common.mappers.ShareMapper;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.service.ShareService;
import com.linkedin.util.Pair;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.model.ExecuteShareResult;
import io.datahubproject.integrations.model.LineageDirection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ShareEntityResolver implements DataFetcher<CompletableFuture<ShareEntityResult>> {

  private final ShareService _shareService;
  private final IntegrationsService _integrationsService;

  @Override
  public CompletableFuture<ShareEntityResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final ShareEntityInput input =
        bindArgument(environment.getArgument("input"), ShareEntityInput.class);
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());
    if (!AuthorizationUtils.canShareEntity(entityUrn, context)) {
      throw new AuthorizationException(
          "Unauthorized to share this entity. Please contact your DataHub administrator.");
    }
    final ShareLineageDirection lineageDirection = input.getLineageDirection();
    LineageDirection shareLineageDirection;
    if (lineageDirection != null) {
      shareLineageDirection = LineageDirection.valueOf(lineageDirection.toString());
    } else {
      shareLineageDirection = null;
    }
    final Urn connectionUrn =
        input.getConnectionUrn() != null ? UrnUtils.getUrn(input.getConnectionUrn()) : null;
    final List<Urn> connectionUrns =
        input.getConnectionUrns() != null
            ? input.getConnectionUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList())
            : new ArrayList<>();

    if (connectionUrn != null) {
      connectionUrns.add(connectionUrn);
    }

    if (connectionUrns.isEmpty()) {
      throw new RuntimeException("No connection urns provided to share this entity with");
    }
    Urn actor = UrnUtils.getUrn(context.getActorUrn());
    List<CompletableFuture<Pair<Urn, ExecuteShareResult>>> shareResultsList =
        connectionUrns.stream()
            .map(
                connectionUrnItr ->
                    _integrationsService.shareEntity(
                        connectionUrnItr, entityUrn, actor, shareLineageDirection))
            .collect(Collectors.toList());

    CompletableFuture<List<Pair<Urn, ExecuteShareResult>>> shareResultsFuture =
        GraphQLConcurrencyUtils.sequence(shareResultsList);

    return shareResultsFuture.thenCompose(
        shareResultPairs ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try {
                    boolean succeeded = true;

                    for (Pair<Urn, ExecuteShareResult> pair : shareResultPairs) {
                      Urn connection = pair.getFirst();
                      ExecuteShareResult shareResult = pair.getSecond();
                      // if the result is null, we know the integrations service failed to share
                      if (shareResult == null) {
                        succeeded = false;
                        _shareService.upsertShareResult(
                            context.getOperationContext(),
                            entityUrn,
                            connection,
                            ShareResultState.FAILURE);
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
                        String.format("Failed to share entity with input %s", input), e);
                  }
                },
                this.getClass().getSimpleName(),
                "get"));
  }
}
