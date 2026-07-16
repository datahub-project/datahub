package com.linkedin.datahub.graphql.resolvers.knowledge;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for soft deleting a particular Document by setting the Status aspect removed
 * field to true. Requires the GET_ENTITY metadata privilege on the document or the MANAGE_DOCUMENTS
 * platform privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class DeleteDocumentResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DocumentService _documentService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String documentUrnString = environment.getArgument("urn");
    final Urn documentUrn = UrnUtils.getUrn(documentUrnString);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canDeleteDocument(documentUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            // Delete using service
            _documentService.deleteDocument(context.getOperationContext(), documentUrn);

            return true;
          } catch (Exception e) {
            log.error(
                "Failed to delete Document with URN {}: {}", documentUrnString, e.getMessage());
            throw new RuntimeException(
                String.format("Failed to delete Document with urn %s", documentUrnString), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
