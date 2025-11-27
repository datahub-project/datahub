package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DocumentState;
import com.linkedin.datahub.graphql.generated.UpdateDocumentStatusInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateDocumentStatusResolverTest {

  private static final String TEST_ARTICLE_URN = "urn:li:document:test-document-123";

  private DocumentService mockService;
  private DataFetchingEnvironment mockEnv;
  private UpdateDocumentStatusResolver resolver;

  @BeforeMethod
  public void setUp() {
    mockService = mock(DocumentService.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    resolver = new UpdateDocumentStatusResolver(mockService);
  }

  @Test
  public void testConstructor() {
    assertNotNull(resolver);
  }

  @Test
  public void testUpdateStatusSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentStatusInput input = new UpdateDocumentStatusInput();
    input.setUrn(TEST_ARTICLE_URN);
    input.setState(DocumentState.PUBLISHED);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Execute
    Boolean result = resolver.get(mockEnv).get();

    // Verify
    assertTrue(result);
    verify(mockService, times(1))
        .updateDocumentStatus(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_ARTICLE_URN)),
            eq(com.linkedin.knowledge.DocumentState.PUBLISHED),
            any(Urn.class));
  }

  @Test
  public void testUpdateStatusUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentStatusInput input = new UpdateDocumentStatusInput();
    input.setUrn(TEST_ARTICLE_URN);
    input.setState(DocumentState.UNPUBLISHED);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Execute and expect exception
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify service was NOT called
    verify(mockService, times(0))
        .updateDocumentStatus(any(OperationContext.class), any(), any(), any());
  }

  @Test
  public void testUpdateStatusServiceException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentStatusInput input = new UpdateDocumentStatusInput();
    input.setUrn(TEST_ARTICLE_URN);
    input.setState(DocumentState.PUBLISHED);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Make service throw exception
    doThrow(new RuntimeException("Service error"))
        .when(mockService)
        .updateDocumentStatus(any(OperationContext.class), any(Urn.class), any(), any(Urn.class));

    // Execute and expect exception
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
