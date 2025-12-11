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
import com.linkedin.datahub.graphql.generated.MergeDraftInput;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class MergeDraftResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DocumentService _documentService;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final MergeDraftInput input =
        bindArgument(environment.getArgument("input"), MergeDraftInput.class);
    final Urn draftUrn = UrnUtils.getUrn(input.getDraftUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canEditDocument(draftUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            final OperationContext opContext = context.getOperationContext();
            final boolean deleteDraft = input.getDeleteDraft() == null || input.getDeleteDraft();
            final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());

            _documentService.mergeDraftIntoParent(opContext, draftUrn, deleteDraft, actorUrn);
            return true;
          } catch (Exception e) {
            log.error("Failed to merge draft {}: {}", input.getDraftUrn(), e.toString());
            throw new RuntimeException("Failed to merge draft: " + e.getMessage(), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
