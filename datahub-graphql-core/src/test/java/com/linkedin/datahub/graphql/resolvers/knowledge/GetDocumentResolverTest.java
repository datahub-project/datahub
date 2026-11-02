package com.linkedin.datahub.graphql.resolvers.knowledge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.types.knowledge.DocumentType;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoaderRegistry;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class GetDocumentResolverTest {

  private static final String DOCUMENT_URN = "urn:li:document:private-document";

  @Test
  public void testGetDeniedBeforeLoading() {
    QueryContext context = mock(QueryContext.class);
    DataLoaderRegistry loaderRegistry = mock(DataLoaderRegistry.class);
    DataFetchingEnvironment environment = mock(DataFetchingEnvironment.class);
    when(environment.getArgument("urn")).thenReturn(DOCUMENT_URN);
    when(environment.getContext()).thenReturn(context);
    when(environment.getDataLoaderRegistry()).thenReturn(loaderRegistry);

    Urn documentUrn = UrnUtils.getUrn(DOCUMENT_URN);
    try (MockedStatic<AuthorizationUtils> authorizationUtils =
        mockStatic(AuthorizationUtils.class)) {
      authorizationUtils
          .when(() -> AuthorizationUtils.canGetDocument(eq(documentUrn), any()))
          .thenReturn(false);

      GetDocumentResolver resolver =
          new GetDocumentResolver(new DocumentType(mock(EntityClient.class)));

      assertThrows(AuthorizationException.class, () -> resolver.get(environment));
      verifyNoInteractions(loaderRegistry);
    }
  }
}
