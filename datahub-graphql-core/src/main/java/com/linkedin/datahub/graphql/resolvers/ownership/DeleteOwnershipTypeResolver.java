package com.linkedin.datahub.graphql.resolvers.ownership;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.service.OwnershipTypeService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteOwnershipTypeResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final OwnershipTypeService _ownershipTypeService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String ownershipTypeUrn = environment.getArgument("urn");
    final Urn urn = UrnUtils.getUrn(ownershipTypeUrn);
    // By default, delete references
    final boolean deleteReferences =
        environment.getArgument("deleteReferences") == null
            ? true
            : environment.getArgument("deleteReferences");

    if (!AuthorizationUtils.canManageOwnershipTypes(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            _ownershipTypeService.deleteOwnershipType(
                urn, deleteReferences, context.getAuthentication());
            log.info(String.format("Successfully deleted ownership type %s with urn", urn));
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to delete ownership type with urn %s", ownershipTypeUrn), e);
          }
        });
  }
}
