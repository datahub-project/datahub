package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateDocumentRelatedEntitiesInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for updating the related entities (assets and documents) of a Document on DataHub.
 * Requires the EDIT_ENTITY_DOCS or EDIT_ENTITY metadata privilege on the document, or
 * MANAGE_DOCUMENTS platform privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateDocumentRelatedEntitiesResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final DocumentService _documentService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateDocumentRelatedEntitiesInput input =
        bindArgument(environment.getArgument("input"), UpdateDocumentRelatedEntitiesInput.class);

    final Urn documentUrn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canEditDocument(documentUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            // Extract URNs
            final List<Urn> relatedAssetUrns =
                input.getRelatedAssets() != null
                    ? input.getRelatedAssets().stream()
                        .map(UrnUtils::getUrn)
                        .collect(Collectors.toList())
                    : null;

            final List<Urn> relatedDocumentUrns =
                input.getRelatedDocuments() != null
                    ? input.getRelatedDocuments().stream()
                        .map(UrnUtils::getUrn)
                        .collect(Collectors.toList())
                    : null;

            // Update using service
            _documentService.updateDocumentRelatedEntities(
                context.getOperationContext(),
                documentUrn,
                relatedAssetUrns,
                relatedDocumentUrns,
                UrnUtils.getUrn(context.getActorUrn()));

            return true;
          } catch (Exception e) {
            log.error(
                "Failed to update related entities for Document with URN {}: {}",
                input.getUrn(),
                e.getMessage());
            throw new RuntimeException(
                String.format("Failed to update Document related entities: %s", e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
