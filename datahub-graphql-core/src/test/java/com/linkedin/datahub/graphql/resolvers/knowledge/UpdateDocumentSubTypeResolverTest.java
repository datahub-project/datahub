package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateDocumentSubTypeInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateDocumentSubTypeResolverTest {

  private static final String TEST_DOCUMENT_URN = "urn:li:document:test-document-123";
  private static final String TEST_SUB_TYPE = "FAQ";

  private DocumentService mockService;
  private DataFetchingEnvironment mockEnv;
  private UpdateDocumentSubTypeResolver resolver;

  @BeforeMethod
  public void setUp() {
    mockService = mock(DocumentService.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    resolver = new UpdateDocumentSubTypeResolver(mockService);
  }

  @Test
  public void testConstructor() {
    assertNotNull(resolver);
  }

  @Test
  public void testUpdateSubTypeSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentSubTypeInput input = new UpdateDocumentSubTypeInput();
    input.setUrn(TEST_DOCUMENT_URN);
    input.setSubType(TEST_SUB_TYPE);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Execute
    Boolean result = resolver.get(mockEnv).get();

    // Verify
    assertTrue(result);
    verify(mockService, times(1))
        .updateDocumentSubType(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_DOCUMENT_URN)),
            eq(TEST_SUB_TYPE),
            any(Urn.class));
  }

  @Test
  public void testUpdateSubTypeUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentSubTypeInput input = new UpdateDocumentSubTypeInput();
    input.setUrn(TEST_DOCUMENT_URN);
    input.setSubType(TEST_SUB_TYPE);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Execute and expect exception
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify service was NOT called
    verify(mockService, times(0))
        .updateDocumentSubType(any(OperationContext.class), any(), any(), any());
  }

  @Test
  public void testUpdateSubTypeServiceException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentSubTypeInput input = new UpdateDocumentSubTypeInput();
    input.setUrn(TEST_DOCUMENT_URN);
    input.setSubType(TEST_SUB_TYPE);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Make service throw exception
    doThrow(new RuntimeException("Service error"))
        .when(mockService)
        .updateDocumentSubType(
            any(OperationContext.class), any(Urn.class), any(String.class), any(Urn.class));

    // Execute and expect exception
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testUpdateSubTypeWithCustomType() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    String customType = "Custom Knowledge Article";

    // Setup input
    UpdateDocumentSubTypeInput input = new UpdateDocumentSubTypeInput();
    input.setUrn(TEST_DOCUMENT_URN);
    input.setSubType(customType);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Execute
    Boolean result = resolver.get(mockEnv).get();

    // Verify
    assertTrue(result);
    verify(mockService, times(1))
        .updateDocumentSubType(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_DOCUMENT_URN)),
            eq(customType),
            any(Urn.class));
  }
}
