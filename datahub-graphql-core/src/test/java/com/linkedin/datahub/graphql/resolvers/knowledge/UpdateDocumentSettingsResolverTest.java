package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateDocumentSettingsInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateDocumentSettingsResolverTest {

  private static final String TEST_DOCUMENT_URN = "urn:li:document:test-document-123";

  private DocumentService mockService;
  private DataFetchingEnvironment mockEnv;
  private UpdateDocumentSettingsResolver resolver;

  @BeforeMethod
  public void setUp() {
    mockService = mock(DocumentService.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    resolver = new UpdateDocumentSettingsResolver(mockService);
  }

  @Test
  public void testConstructor() {
    assertNotNull(resolver);
  }

  @Test
  public void testUpdateSettingsSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentSettingsInput input = new UpdateDocumentSettingsInput();
    input.setUrn(TEST_DOCUMENT_URN);
    input.setShowInGlobalContext(false);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Execute
    Boolean result = resolver.get(mockEnv).get();

    // Verify
    assertTrue(result);

    // Capture the settings argument to verify its content
    com.linkedin.knowledge.DocumentSettings expectedSettings =
        new com.linkedin.knowledge.DocumentSettings();
    expectedSettings.setShowInGlobalContext(false);

    verify(mockService, times(1))
        .updateDocumentSettings(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_DOCUMENT_URN)),
            eq(expectedSettings),
            any(Urn.class));
  }

  @Test
  public void testUpdateSettingsSuccessTrue() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentSettingsInput input = new UpdateDocumentSettingsInput();
    input.setUrn(TEST_DOCUMENT_URN);
    input.setShowInGlobalContext(true);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Execute
    Boolean result = resolver.get(mockEnv).get();

    // Verify
    assertTrue(result);

    com.linkedin.knowledge.DocumentSettings expectedSettings =
        new com.linkedin.knowledge.DocumentSettings();
    expectedSettings.setShowInGlobalContext(true);

    verify(mockService, times(1))
        .updateDocumentSettings(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_DOCUMENT_URN)),
            eq(expectedSettings),
            any(Urn.class));
  }

  @Test
  public void testUpdateSettingsUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentSettingsInput input = new UpdateDocumentSettingsInput();
    input.setUrn(TEST_DOCUMENT_URN);
    input.setShowInGlobalContext(true);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Execute and expect exception
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify service was NOT called
    verify(mockService, times(0))
        .updateDocumentSettings(any(OperationContext.class), any(), any(), any());
  }

  @Test
  public void testUpdateSettingsServiceException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Setup input
    UpdateDocumentSettingsInput input = new UpdateDocumentSettingsInput();
    input.setUrn(TEST_DOCUMENT_URN);
    input.setShowInGlobalContext(true);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Make service throw exception
    doThrow(new RuntimeException("Service error"))
        .when(mockService)
        .updateDocumentSettings(any(OperationContext.class), any(Urn.class), any(), any(Urn.class));

    // Execute and expect exception
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
