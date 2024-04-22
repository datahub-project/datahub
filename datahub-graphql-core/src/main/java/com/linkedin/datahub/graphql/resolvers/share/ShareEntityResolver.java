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
    final Authentication authentication = context.getAuthentication();
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());
    final Urn connectionUrn =
        input.getConnectionUrn() != null ? UrnUtils.getUrn(input.getConnectionUrn()) : null;
    final List<Urn> connectionUrns =
        input.getConnectionUrns() != null
            ? input.getConnectionUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList())
            : new ArrayList<>();

    if (connectionUrn != null) {
      connectionUrns.add(connectionUrn);
    }

    return CompletableFuture.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canShareEntity(entityUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to share this entity. Please contact your DataHub administrator.");
          }
          if (connectionUrns.size() == 0) {
            throw new RuntimeException("No connection urns provided to share this entity with");
          }
          try {
            boolean succeeded = true;
            for (Urn connection : connectionUrns) {
              // integrations service will update the share aspect of all entities if successful
              ExecuteShareResult result = _integrationsService.shareEntity(connection, entityUrn);
              // if the result is null, we know the integrations service failed to share
              if (result == null) {
                succeeded = false;
                _shareService.upsertShareResult(
                    entityUrn, connection, ShareResultState.FAILURE, authentication);
              }
            }
            ;
            Share shareAspect = _shareService.getShareOrDefault(entityUrn, authentication);
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
