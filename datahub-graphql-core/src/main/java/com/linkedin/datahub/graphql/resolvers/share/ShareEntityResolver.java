package com.linkedin.datahub.graphql.resolvers.share;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.linkedin.common.Share;
import com.linkedin.common.ShareResultState;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ShareEntityInput;
import com.linkedin.datahub.graphql.generated.ShareEntityResult;
import com.linkedin.datahub.graphql.types.common.mappers.ShareMapper;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.service.ShareService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.model.ExecuteShareResult;
import java.util.concurrent.CompletableFuture;
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
    final Authentication authentication = context.getAuthentication();
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());
    final Urn connectionUrn = UrnUtils.getUrn(input.getConnectionUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canShareEntity(entityUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to share this entity. Please contact your DataHub administrator.");
          }
          try {
            // integrations service will update the share aspect of all entities if successful
            ExecuteShareResult result = _integrationsService.shareEntity(connectionUrn, entityUrn);
            Share shareAspect;
            boolean succeeded = true;
            // if the result is null, we know the integrations service failed to share
            if (result == null) {
              succeeded = false;
              shareAspect =
                  _shareService.upsertShareResult(
                      entityUrn, connectionUrn, ShareResultState.FAILURE, authentication);
            } else {
              shareAspect = _shareService.getShareOrDefault(entityUrn, authentication);
            }
            ShareEntityResult shareEntityResult = new ShareEntityResult();
            shareEntityResult.setSucceeded(succeeded);
            shareEntityResult.setShare(ShareMapper.map(context, shareAspect));
            return shareEntityResult;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to share entity with input %s", input), e);
          }
        });
  }
}
