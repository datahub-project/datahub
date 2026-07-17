package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;
import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ParentDocumentsResult;
import com.linkedin.datahub.graphql.types.knowledge.DocumentMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class ParentDocumentsResolver
    implements DataFetcher<CompletableFuture<ParentDocumentsResult>> {

  private final EntityClient _entityClient;

  public ParentDocumentsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  private void aggregateParentDocuments(
      List<Document> documents, String startUrn, QueryContext context) {
    // The parent chain is a linked list (each node points to its parent), so the walk is
    // inherently sequential and cannot be batched. It can, however, fetch each node exactly once:
    // an ancestor's full response already carries its DocumentInfo, so there is no need to
    // re-fetch it to find the next hop (the previous per-level double getV2).
    Set<String> visitedUrns = new HashSet<>();
    visitedUrns.add(startUrn);
    try {
      Urn sourceUrn = new Urn(startUrn);
      // The source entity itself is not part of the result; fetch only its DocumentInfo to find
      // the first parent link.
      EntityResponse response =
          _entityClient.getV2(
              context.getOperationContext(),
              sourceUrn.getEntityType(),
              sourceUrn,
              Collections.singleton(DOCUMENT_INFO_ASPECT_NAME));

      int depth = 0;
      while (response != null
          && response.getAspects().containsKey(DOCUMENT_INFO_ASPECT_NAME)
          && depth < context.getMaxParentDepth()) {
        DataMap dataMap = response.getAspects().get(DOCUMENT_INFO_ASPECT_NAME).getValue().data();
        com.linkedin.knowledge.DocumentInfo documentInfo =
            new com.linkedin.knowledge.DocumentInfo(dataMap);
        if (!documentInfo.hasParentDocument()) {
          break;
        }
        Urn parentUrn = documentInfo.getParentDocument().getDocument();
        if (!visitedUrns.add(parentUrn.toString())) {
          break;
        }
        // Fetch the parent's full aspects once: used both to map it into a Document and, on the
        // next iteration, to read its DocumentInfo for the following hop.
        response =
            _entityClient.getV2(
                context.getOperationContext(), parentUrn.getEntityType(), parentUrn, null);
        if (response == null) {
          break;
        }
        documents.add(DocumentMapper.map(context, response));
        depth++;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve parent documents from GMS", e);
    }
  }

  @Override
  public CompletableFuture<ParentDocumentsResult> get(DataFetchingEnvironment environment) {

    final QueryContext context = getQueryContext(environment);
    final String urn = ((Entity) environment.getSource()).getUrn();
    final List<Document> documents = new ArrayList<>();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            aggregateParentDocuments(documents, urn, context);
            final ParentDocumentsResult result = new ParentDocumentsResult();

            List<Document> viewable = new ArrayList<>(documents);

            result.setCount(viewable.size());
            result.setDocuments(viewable);
            return result;
          } catch (DataHubGraphQLException e) {
            throw new RuntimeException("Failed to load all parent documents", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
