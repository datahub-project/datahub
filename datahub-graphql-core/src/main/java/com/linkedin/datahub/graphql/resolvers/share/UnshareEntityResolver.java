package com.linkedin.datahub.graphql.resolvers.share;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.linkedin.common.Share;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ShareEntityResult;
import com.linkedin.datahub.graphql.generated.UnshareEntityInput;
import com.linkedin.datahub.graphql.types.common.mappers.ShareMapper;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.service.ShareService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.model.ExecuteUnshareResult;
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
    final List<Urn> connectionUrns =
        input.getConnectionUrns().stream()
            .map(urn -> UrnUtils.getUrn(urn))
            .collect(Collectors.toList());

    return CompletableFuture.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canShareEntity(entityUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to unshare this entity. Please contact your DataHub administrator.");
          }
          try {
            // integrations service will update the share aspect of all entities if successful
            boolean succeeded = true;
            for (Urn connectionUrn : connectionUrns) {
              ExecuteUnshareResult result =
                  _integrationsService.unshareEntity(connectionUrn, entityUrn);
              // if the result is null, we know the integrations service failed to unshare
              if (result == null) {
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
        });
  }
}
