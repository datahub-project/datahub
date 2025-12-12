package com.linkedin.datahub.graphql.resolvers.knowledge;

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
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ParentDocumentsResolver
    implements DataFetcher<CompletableFuture<ParentDocumentsResult>> {

  private final EntityClient _entityClient;

  public ParentDocumentsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  private void aggregateParentDocuments(
      List<Document> documents, String urn, QueryContext context) {
    try {
      Urn entityUrn = new Urn(urn);
      EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(DOCUMENT_INFO_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(DOCUMENT_INFO_ASPECT_NAME)) {
        DataMap dataMap =
            entityResponse.getAspects().get(DOCUMENT_INFO_ASPECT_NAME).getValue().data();
        com.linkedin.knowledge.DocumentInfo documentInfo =
            new com.linkedin.knowledge.DocumentInfo(dataMap);
        if (documentInfo.hasParentDocument()) {
          Urn parentUrn = documentInfo.getParentDocument().getDocument();
          EntityResponse response =
              _entityClient.getV2(
                  context.getOperationContext(), parentUrn.getEntityType(), parentUrn, null);
          if (response != null) {
            Document mappedDocument = DocumentMapper.map(context, response);
            documents.add(mappedDocument);
            aggregateParentDocuments(documents, mappedDocument.getUrn(), context);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public CompletableFuture<ParentDocumentsResult> get(DataFetchingEnvironment environment) {

    final QueryContext context = environment.getContext();
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
