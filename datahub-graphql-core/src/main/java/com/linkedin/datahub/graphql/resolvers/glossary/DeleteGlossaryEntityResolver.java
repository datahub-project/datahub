package com.linkedin.datahub.graphql.resolvers.glossary;

import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteGlossaryEntityResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService<?> _entityService;
  private final EntityVersioningService _entityVersioningService;

  public DeleteGlossaryEntityResolver(
      final EntityClient entityClient,
      EntityService<?> entityService,
      final EntityVersioningService entityVersioningService) {
    _entityClient = entityClient;
    _entityService = entityService;
    _entityVersioningService = entityVersioningService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn entityUrn = Urn.createFromString(environment.getArgument("urn"));

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (GlossaryUtils.canUpdateGlossaryEntity(entityUrn, context, _entityClient)) {
            if (!_entityService.exists(context.getOperationContext(), entityUrn, true)) {
              throw new RuntimeException(String.format("This urn does not exist: %s", entityUrn));
            }

            try {
              unlinkFromVersionSet(context, entityUrn);
              final var references =
                  _entityClient.deleteEntityThenReferences(
                      context.getOperationContext(), entityUrn);

              // Asynchronously Delete all references to the entity (to return quickly)
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      references.run();
                    } catch (Exception e) {
                      log.error(
                          String.format(
                              "Caught exception while attempting to clear all entity references for glossary entity with urn %s",
                              entityUrn),
                          e);
                    }
                  });

              return true;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format(
                      "Failed to perform delete against glossary entity with urn %s", entityUrn),
                  e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Deleting a version must first unlink it so the version set hands "latest" to the next version.
   * Otherwise the set keeps pointing at the deleted entity and its remaining versions stay hidden
   * from search, which filters out non-latest versions.
   */
  private void unlinkFromVersionSet(final QueryContext context, final Urn entityUrn) {
    final VersionProperties versionProperties =
        (VersionProperties)
            _entityService.getAspect(
                context.getOperationContext(),
                entityUrn,
                Constants.VERSION_PROPERTIES_ASPECT_NAME,
                0L);
    if (versionProperties != null) {
      _entityVersioningService.unlinkVersion(
          context.getOperationContext(), versionProperties.getVersionSet(), entityUrn);
    }
  }
}
