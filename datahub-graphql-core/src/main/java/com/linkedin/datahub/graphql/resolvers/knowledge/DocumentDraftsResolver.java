/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.knowledge;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DocumentDraftsResolver implements DataFetcher<CompletableFuture<List<Document>>> {

  // TODO: This is a temporary limit for V1, if we need to support more drafts, we need to add
  // pagination to this resolver.
  private static final int MAX_DRAFTS = 1000;
  private final DocumentService _documentService;

  @Override
  public CompletableFuture<List<Document>> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final OperationContext opContext = context.getOperationContext();
    final Document source = environment.getSource();
    final Urn publishedUrn = UrnUtils.getUrn(source.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            var searchResult =
                _documentService.getDraftDocuments(opContext, publishedUrn, 0, MAX_DRAFTS);
            return searchResult.getEntities().stream()
                .map(
                    entity -> {
                      Document doc = new Document();
                      doc.setUrn(entity.getEntity().toString());
                      // Type is resolved downstream when hydrated; set as DOCUMENT for consistency
                      doc.setType(com.linkedin.datahub.graphql.generated.EntityType.DOCUMENT);
                      return doc;
                    })
                .collect(Collectors.toList());
          } catch (Exception e) {
            throw new RuntimeException("Failed to fetch draft documents: " + e.getMessage(), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
