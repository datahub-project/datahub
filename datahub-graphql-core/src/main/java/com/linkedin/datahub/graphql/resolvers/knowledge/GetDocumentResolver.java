package com.linkedin.datahub.graphql.resolvers.knowledge;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import com.linkedin.datahub.graphql.types.knowledge.DocumentType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

/** Resolves a document by URN after enforcing the explicit document READ policy. */
public class GetDocumentResolver implements DataFetcher<CompletableFuture<Document>> {

  private final LoadableTypeResolver<Document, String> _delegate;

  public GetDocumentResolver(DocumentType documentType) {
    _delegate =
        new LoadableTypeResolver<>(documentType, environment -> environment.getArgument("urn"));
  }

  @Override
  public CompletableFuture<Document> get(DataFetchingEnvironment environment) throws Exception {
    final String urnString = environment.getArgument("urn");
    if (urnString == null) {
      return null;
    }

    final Urn documentUrn = UrnUtils.getUrn(urnString);
    final QueryContext context = environment.getContext();
    if (!AuthorizationUtils.canGetDocument(documentUrn, context)) {
      throw new AuthorizationException("Unauthorized to view document: " + documentUrn);
    }
    return _delegate.get(environment);
  }
}
