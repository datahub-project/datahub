package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.service.DocumentService;
import com.linkedin.metadata.service.SearchIndexMode;
import com.linkedin.metadata.service.ServiceAuthorizationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteDocumentResolverTest {

  private static final String TEST_ARTICLE_URN = "urn:li:document:test-document";

  private DocumentService mockService;
  private DeleteDocumentResolver resolver;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
  public void setupTest() {
    mockService = mock(DocumentService.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    resolver = new DeleteDocumentResolver(mockService);
  }

  @Test
  public void testDeleteArticleSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_ARTICLE_URN);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify service was called
    verify(mockService, times(1))
        .deleteDocument(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_ARTICLE_URN)),
            eq(SearchIndexMode.SYNC));
  }

  @Test
  public void testDeleteArticleUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_ARTICLE_URN);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify service was NOT called
    verify(mockService, times(0))
        .deleteDocument(any(OperationContext.class), any(Urn.class), any(SearchIndexMode.class));
  }

  @Test
  public void testDeleteArticleServiceThrowsException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_ARTICLE_URN);

    doThrow(new RuntimeException("Service error"))
        .when(mockService)
        .deleteDocument(any(OperationContext.class), any(Urn.class), eq(SearchIndexMode.SYNC));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testDeleteDocumentServiceAuthorizationFailureIsPreserved() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_ARTICLE_URN);
    doThrow(new ServiceAuthorizationException("denied"))
        .when(mockService)
        .deleteDocument(any(OperationContext.class), any(Urn.class), eq(SearchIndexMode.SYNC));

    CompletionException exception =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    assertTrue(exception.getCause() instanceof AuthorizationException);
  }
}
