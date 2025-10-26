package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DocumentContentInput;
import com.linkedin.datahub.graphql.generated.UpdateDocumentContentsInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateDocumentContentsResolverTest {

  private static final String TEST_ARTICLE_URN = "urn:li:document:test-document";

  private DocumentService mockService;
  private UpdateDocumentContentsResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private UpdateDocumentContentsInput input;

  @BeforeMethod
  public void setupTest() {
    mockService = mock(DocumentService.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    // Setup default input
    input = new UpdateDocumentContentsInput();
    input.setUrn(TEST_ARTICLE_URN);

    DocumentContentInput contentInput = new DocumentContentInput();
    contentInput.setText("Updated content");
    input.setContents(contentInput);

    resolver = new UpdateDocumentContentsResolver(mockService);
  }

  @Test
  public void testUpdateContentsSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify service was called
    verify(mockService, times(1))
        .updateDocumentContents(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_ARTICLE_URN)),
            any(),
            eq(null),
            any(Urn.class));
  }

  @Test
  public void testUpdateContentsWithTitle() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setTitle("New Title");

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify title was passed to service
    verify(mockService, times(1))
        .updateDocumentContents(
            any(OperationContext.class), any(Urn.class), any(), eq("New Title"), any(Urn.class));
  }

  @Test
  public void testUpdateContentsUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify service was NOT called
    verify(mockService, times(0))
        .updateDocumentContents(any(OperationContext.class), any(), any(), any(), any());
  }

  @Test
  public void testUpdateContentsServiceThrowsException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    doThrow(new RuntimeException("Service error"))
        .when(mockService)
        .updateDocumentContents(any(OperationContext.class), any(), any(), any(), any());

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
