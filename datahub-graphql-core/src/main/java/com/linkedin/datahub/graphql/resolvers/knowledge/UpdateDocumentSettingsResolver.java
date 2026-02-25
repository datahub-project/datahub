package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateDocumentSettingsInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for updating the settings of a Document on DataHub. Requires the EDIT_ENTITY_DOCS
 * or EDIT_ENTITY privilege for the document or MANAGE_DOCUMENTS privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateDocumentSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DocumentService _documentService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateDocumentSettingsInput input =
        bindArgument(environment.getArgument("input"), UpdateDocumentSettingsInput.class);

    final Urn documentUrn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Use the same authorization check as update operations - need to edit the document
          if (!AuthorizationUtils.canEditDocument(documentUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            final com.linkedin.knowledge.DocumentSettings settings =
                new com.linkedin.knowledge.DocumentSettings();
            settings.setShowInGlobalContext(input.getShowInGlobalContext());

            _documentService.updateDocumentSettings(
                context.getOperationContext(),
                documentUrn,
                settings,
                UrnUtils.getUrn(context.getActorUrn()));

            return true;
          } catch (Exception e) {
            log.error(
                "Failed to update settings for document {}. Error: {}",
                input.getUrn(),
                e.getMessage());
            throw new RuntimeException(
                String.format("Failed to update settings for document %s", input.getUrn()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
