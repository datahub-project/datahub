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
import com.linkedin.datahub.graphql.generated.MoveDocumentInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for moving a Document to a different parent (or to root level if no parent is
 * specified). Requires the EDIT_ENTITY_DOCS or EDIT_ENTITY metadata privilege on the document, or
 * MANAGE_DOCUMENTS platform privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class MoveDocumentResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DocumentService _documentService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final MoveDocumentInput input =
        bindArgument(environment.getArgument("input"), MoveDocumentInput.class);

    final Urn documentUrn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canEditDocument(documentUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            final Urn newParentUrn =
                input.getParentDocument() != null
                    ? UrnUtils.getUrn(input.getParentDocument())
                    : null;

            // Move using service
            _documentService.moveDocument(
                context.getOperationContext(),
                documentUrn,
                newParentUrn,
                UrnUtils.getUrn(context.getActorUrn()));

            return true;
          } catch (Exception e) {
            log.error("Failed to move Document with URN {}: {}", input.getUrn(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to move Document: %s", e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
