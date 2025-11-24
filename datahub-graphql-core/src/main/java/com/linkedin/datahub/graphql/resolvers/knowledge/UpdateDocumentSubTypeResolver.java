package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateDocumentSubTypeInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for updating the sub-type of a Document on DataHub. Requires the EDIT_ENTITY_DOCS
 * or EDIT_ENTITY privilege for the document or MANAGE_DOCUMENTS privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateDocumentSubTypeResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DocumentService _documentService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateDocumentSubTypeInput input =
        bindArgument(environment.getArgument("input"), UpdateDocumentSubTypeInput.class);

    final Urn documentUrn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Use the same authorization check as update operations - need to edit the document
          if (!AuthorizationUtils.canEditDocument(documentUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            _documentService.updateDocumentSubType(
                context.getOperationContext(),
                documentUrn,
                input.getSubType(),
                UrnUtils.getUrn(context.getActorUrn()));

            return true;
          } catch (Exception e) {
            log.error(
                "Failed to update sub-type for document {}. Error: {}",
                input.getUrn(),
                e.getMessage());
            throw new RuntimeException(
                String.format("Failed to update sub-type for document %s", input.getUrn()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
