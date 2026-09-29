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
import com.linkedin.datahub.graphql.generated.CreateDocumentInput;
import com.linkedin.datahub.graphql.generated.DocumentContentInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DocumentService;
import com.linkedin.metadata.service.SearchIndexMode;
import com.linkedin.metadata.service.ServiceAuthorizationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateDocumentResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_DOCUMENT_URN = UrnUtils.getUrn("urn:li:document:test-document");

  private DocumentService mockService;
  private EntityService mockEntityService;
  private CreateDocumentResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private CreateDocumentInput input;

  @BeforeMethod
  public void setupTest() throws Exception {
    mockService = mock(DocumentService.class);
    mockEntityService = mock(EntityService.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    // Setup default input
    input = new CreateDocumentInput();
    input.setSubType("tutorial");
    input.setTitle("Test Document");

    DocumentContentInput contentInput = new DocumentContentInput();
    contentInput.setText("Test content");
    input.setContents(contentInput);

    // Mock the service to return a test URN
    when(mockService.createDocument(
            any(OperationContext.class),
            any(), // id
            any(), // subTypes list
            any(), // title
            any(), // source
            any(), // state
            any(), // content
            any(), // parent
            any(), // related assets
            any(), // related documents
            any(), // showInGlobalContext
            any(), // owners
            any(Urn.class),
            eq(SearchIndexMode.SYNC))) // actor
        .thenReturn(TEST_DOCUMENT_URN);

    resolver = new CreateDocumentResolver(mockService, mockEntityService);
  }

  @Test
  public void testCreateDocumentSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN.toString());

    String result = resolver.get(mockEnv).get();

    assertEquals(result, TEST_DOCUMENT_URN.toString());

    // Verify service was called with NATIVE source type
    verify(mockService, times(1))
        .createDocument(
            any(OperationContext.class),
            any(), // id
            any(), // subTypes list (contains "tutorial")
            eq("Test Document"), // title
            argThat(
                source ->
                    source != null
                        && source.getSourceType()
                            == com.linkedin.knowledge.DocumentSourceType.NATIVE), // source must be
            // NATIVE
            any(), // state parameter
            any(), // contents
            any(), // parent
            any(), // related assets
            any(), // related documents
            any(), // showInGlobalContext
            any(), // owners
            any(Urn.class),
            eq(SearchIndexMode.SYNC)); // actor URN

    verify(mockService, never())
        .setDocumentOwnership(any(), any(), any(), any(), any(SearchIndexMode.class));
  }

  @Test
  public void testCreateDocumentUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify service was NOT called
    verify(mockService, times(0))
        .createDocument(
            any(OperationContext.class),
            any(), // id
            any(), // subTypes
            any(), // title
            any(), // source
            any(), // state
            any(), // content
            any(), // parent
            any(), // related assets
            any(), // related documents
            any(), // showInGlobalContext
            any(), // owners
            any(),
            any(SearchIndexMode.class)); // actor
  }

  @Test
  public void testCreateDocumentWithCustomId() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN.toString());

    input.setId("custom-id");

    String result = resolver.get(mockEnv).get();

    assertEquals(result, TEST_DOCUMENT_URN.toString());

    // Verify custom ID was passed to service
    verify(mockService, times(1))
        .createDocument(
            any(OperationContext.class),
            eq("custom-id"), // id
            any(), // subTypes
            any(), // title
            any(), // source
            any(), // state
            any(), // content
            any(), // parent
            any(), // related assets
            any(), // related documents
            any(), // showInGlobalContext
            any(), // owners
            any(),
            eq(SearchIndexMode.SYNC)); // actor
  }

  @Test
  public void testCreateDocumentWithParent() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN.toString());

    input.setParentDocument("urn:li:document:parent");

    String result = resolver.get(mockEnv).get();

    assertEquals(result, TEST_DOCUMENT_URN.toString());
  }

  @Test
  public void testCreateDocumentWithCustomOwners() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN.toString());

    // Add custom owners to input
    OwnerInput owner1 = new OwnerInput();
    owner1.setOwnerUrn("urn:li:corpuser:owner1");
    owner1.setOwnerEntityType(OwnerEntityType.CORP_USER);
    owner1.setType(OwnershipType.TECHNICAL_OWNER);

    OwnerInput owner2 = new OwnerInput();
    owner2.setOwnerUrn("urn:li:corpuser:owner2");
    owner2.setOwnerEntityType(OwnerEntityType.CORP_USER);
    owner2.setType(OwnershipType.BUSINESS_OWNER);

    input.setOwners(java.util.Arrays.asList(owner1, owner2));

    String result = resolver.get(mockEnv).get();

    assertEquals(result, TEST_DOCUMENT_URN.toString());

    // Verify custom owners are part of the create operation
    verify(mockService, times(1))
        .createDocument(
            any(OperationContext.class),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            argThat(owners -> owners != null && owners.size() == 2),
            any(Urn.class),
            eq(SearchIndexMode.SYNC));
    verify(mockService, never())
        .setDocumentOwnership(any(), any(), any(), any(), any(SearchIndexMode.class));
  }

  @Test
  public void testCreateDocumentServiceThrowsException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN.toString());

    when(mockService.createDocument(
            any(OperationContext.class),
            any(), // id
            any(), // subTypes
            any(), // title
            any(), // source
            any(), // state
            any(), // content
            any(), // parent
            any(), // related assets
            any(), // related documents
            any(), // showInGlobalContext
            any(), // owners
            any(),
            eq(SearchIndexMode.SYNC))) // actor
        .thenThrow(new RuntimeException("Service error"));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testCreateDocumentServiceAuthorizationFailureIsPreserved() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN.toString());
    when(mockService.createDocument(
            any(OperationContext.class),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            eq(SearchIndexMode.SYNC)))
        .thenThrow(new ServiceAuthorizationException("denied"));

    CompletionException exception =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    assertTrue(exception.getCause() instanceof AuthorizationException);
  }
}
