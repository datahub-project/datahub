/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateDocumentContentsInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for updating the contents of a Document on DataHub. Requires the EDIT_ENTITY_DOCS
 * or EDIT_ENTITY metadata privilege on the document, or MANAGE_DOCUMENTS platform privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateDocumentContentsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DocumentService _documentService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateDocumentContentsInput input =
        bindArgument(environment.getArgument("input"), UpdateDocumentContentsInput.class);

    final Urn documentUrn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canEditDocument(documentUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            // Extract content text (can be null if only updating title or subType)
            final String content =
                input.getContents() != null ? input.getContents().getText() : null;

            // Extract subType and convert to list if provided
            final java.util.List<String> subTypes =
                input.getSubType() != null
                    ? java.util.Collections.singletonList(input.getSubType())
                    : null;

            // Update using service
            _documentService.updateDocumentContents(
                context.getOperationContext(),
                documentUrn,
                content,
                input.getTitle(),
                subTypes,
                UrnUtils.getUrn(context.getActorUrn()));

            return true;
          } catch (Exception e) {
            log.error(
                "Failed to update contents for Document with URN {}: {}",
                input.getUrn(),
                e.getMessage());
            throw new RuntimeException(
                String.format("Failed to update Document contents: %s", e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
