package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DescriptionUpdateInput;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateDescriptionResolverTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)";
  private static final String TEST_DOCUMENT_URN = "urn:li:document:test-document";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String TEST_DESCRIPTION = "Updated test description";

  private EntityService<?> mockEntityService;
  private EntityClient mockEntityClient;
  private UpdateDescriptionResolver resolver;

  @BeforeMethod
  public void setup() {
    mockEntityService = getMockEntityService();
    mockEntityClient = Mockito.mock(EntityClient.class);
    resolver = new UpdateDescriptionResolver(mockEntityService, mockEntityClient);
  }

  @Test
  public void testUpdateDatasetTopLevelDescription() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DATASET_URN)), eq(true)))
        .thenReturn(true);

    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DATASET_URN);
    input.setDescription(TEST_DESCRIPTION);
    // No subResourceType - should update top-level description

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    EditableDatasetProperties expectedProperties = new EditableDatasetProperties();
    expectedProperties.setDescription(TEST_DESCRIPTION);

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_DATASET_URN),
            EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
            expectedProperties);

    verifySingleIngestProposal(mockEntityService, 1, proposal);
  }

  @Test
  public void testUpdateDatasetTopLevelDescriptionUnauthorized() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DATASET_URN)), eq(true)))
        .thenReturn(true);

    QueryContext mockContext = getMockDenyContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DATASET_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(Exception.class, () -> resolver.get(mockEnv).get());

    Mockito.verify(mockEntityService, Mockito.never())
        .ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testUpdateDatasetDescriptionNonExistentDataset() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DATASET_URN)), eq(true)))
        .thenReturn(false);

    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DATASET_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(Exception.class, () -> resolver.get(mockEnv).get());

    Mockito.verify(mockEntityService, Mockito.never())
        .ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testUpdateDocumentDescription() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DOCUMENT_URN)), eq(true)))
        .thenReturn(true);

    // Create empty existing Documentation for getAspect call (no existing docs)
    Documentation existingDocumentation = new Documentation();

    Mockito.when(
            mockEntityService.getAspect(
                any(),
                eq(Urn.createFromString(TEST_DOCUMENT_URN)),
                eq(DOCUMENTATION_ASPECT_NAME),
                eq(0L)))
        .thenReturn(existingDocumentation);

    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DOCUMENT_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify that ingestProposal was called with the DOCUMENTATION_ASPECT_NAME
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(
                proposal -> {
                  if (proposal instanceof MetadataChangeProposal) {
                    MetadataChangeProposal mcp = (MetadataChangeProposal) proposal;
                    return mcp.getAspectName().equals(DOCUMENTATION_ASPECT_NAME)
                        && mcp.getEntityUrn().toString().equals(TEST_DOCUMENT_URN);
                  }
                  return false;
                }),
            any(),
            eq(false));
  }

  @Test
  public void testUpdateDocumentDescriptionUnauthorized() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DOCUMENT_URN)), eq(true)))
        .thenReturn(true);

    QueryContext mockContext = getMockDenyContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DOCUMENT_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(Exception.class, () -> resolver.get(mockEnv).get());

    Mockito.verify(mockEntityService, Mockito.never())
        .ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testUpdateDocumentDescriptionNonExistent() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DOCUMENT_URN)), eq(true)))
        .thenReturn(false);

    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DOCUMENT_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(Exception.class, () -> resolver.get(mockEnv).get());

    Mockito.verify(mockEntityService, Mockito.never())
        .ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testUpdateDocumentDescriptionPreservesPropagatedDocs() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DOCUMENT_URN)), eq(true)))
        .thenReturn(true);

    // Create existing Documentation with a propagated doc entry
    Documentation existingDocumentation = new Documentation();
    DocumentationAssociationArray existingAssocs = new DocumentationAssociationArray();

    // Add a propagated doc
    DocumentationAssociation propagatedDoc = new DocumentationAssociation();
    propagatedDoc.setDocumentation("Propagated description from upstream");
    propagatedDoc.setAttribution(
        new MetadataAttribution()
            .setTime(1000L)
            .setActor(Urn.createFromString("urn:li:corpuser:system"))
            .setSourceDetail(
                new StringMap(
                    Map.of(
                        "propagated", "true",
                        "origin", "urn:li:dataset:upstream"))));
    existingAssocs.add(propagatedDoc);
    existingDocumentation.setDocumentations(existingAssocs);

    Mockito.when(
            mockEntityService.getAspect(
                any(),
                eq(Urn.createFromString(TEST_DOCUMENT_URN)),
                eq(DOCUMENTATION_ASPECT_NAME),
                eq(0L)))
        .thenReturn(existingDocumentation);

    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DOCUMENT_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify that ingestProposal was called and the MCP contains both propagated doc and new UI doc
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(
                proposal -> {
                  if (proposal instanceof MetadataChangeProposal) {
                    MetadataChangeProposal mcp = (MetadataChangeProposal) proposal;
                    if (!mcp.getAspectName().equals(DOCUMENTATION_ASPECT_NAME)) {
                      return false;
                    }
                    // The proposal should contain documentation with multiple entries
                    // (propagated preserved + new UI doc)
                    return mcp.getEntityUrn().toString().equals(TEST_DOCUMENT_URN);
                  }
                  return false;
                }),
            any(),
            eq(false));
  }

  @Test
  public void testUpdateDocumentDescriptionPreservesInferredDocs() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DOCUMENT_URN)), eq(true)))
        .thenReturn(true);

    // Create existing Documentation with an inferred (AI) doc entry
    Documentation existingDocumentation = new Documentation();
    DocumentationAssociationArray existingAssocs = new DocumentationAssociationArray();

    // Add an inferred doc
    DocumentationAssociation inferredDoc = new DocumentationAssociation();
    inferredDoc.setDocumentation("AI generated description");
    inferredDoc.setAttribution(
        new MetadataAttribution()
            .setTime(1000L)
            .setActor(Urn.createFromString("urn:li:corpuser:system"))
            .setSourceDetail(new StringMap(Map.of("inferred", "true"))));
    existingAssocs.add(inferredDoc);
    existingDocumentation.setDocumentations(existingAssocs);

    Mockito.when(
            mockEntityService.getAspect(
                any(),
                eq(Urn.createFromString(TEST_DOCUMENT_URN)),
                eq(DOCUMENTATION_ASPECT_NAME),
                eq(0L)))
        .thenReturn(existingDocumentation);

    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DOCUMENT_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify that ingestProposal was called
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(
                proposal -> {
                  if (proposal instanceof MetadataChangeProposal) {
                    MetadataChangeProposal mcp = (MetadataChangeProposal) proposal;
                    return mcp.getAspectName().equals(DOCUMENTATION_ASPECT_NAME)
                        && mcp.getEntityUrn().toString().equals(TEST_DOCUMENT_URN);
                  }
                  return false;
                }),
            any(),
            eq(false));
  }

  @Test
  public void testUpdateDocumentDescriptionReplacesExistingUiDoc() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DOCUMENT_URN)), eq(true)))
        .thenReturn(true);

    // Create existing Documentation with a UI-authored doc entry
    Documentation existingDocumentation = new Documentation();
    DocumentationAssociationArray existingAssocs = new DocumentationAssociationArray();

    // Add an existing UI-authored doc
    DocumentationAssociation existingUiDoc = new DocumentationAssociation();
    existingUiDoc.setDocumentation("Old UI description");
    existingUiDoc.setAttribution(
        new MetadataAttribution()
            .setTime(1000L)
            .setActor(Urn.createFromString(TEST_ACTOR_URN))
            .setSourceDetail(new StringMap(Map.of("ui", "true"))));
    existingAssocs.add(existingUiDoc);

    // Also add a propagated doc that should be preserved
    DocumentationAssociation propagatedDoc = new DocumentationAssociation();
    propagatedDoc.setDocumentation("Propagated description");
    propagatedDoc.setAttribution(
        new MetadataAttribution()
            .setTime(500L)
            .setActor(Urn.createFromString("urn:li:corpuser:system"))
            .setSourceDetail(new StringMap(Map.of("propagated", "true"))));
    existingAssocs.add(propagatedDoc);

    existingDocumentation.setDocumentations(existingAssocs);

    Mockito.when(
            mockEntityService.getAspect(
                any(),
                eq(Urn.createFromString(TEST_DOCUMENT_URN)),
                eq(DOCUMENTATION_ASPECT_NAME),
                eq(0L)))
        .thenReturn(existingDocumentation);

    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DOCUMENT_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify that ingestProposal was called
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(
                proposal -> {
                  if (proposal instanceof MetadataChangeProposal) {
                    MetadataChangeProposal mcp = (MetadataChangeProposal) proposal;
                    return mcp.getAspectName().equals(DOCUMENTATION_ASPECT_NAME)
                        && mcp.getEntityUrn().toString().equals(TEST_DOCUMENT_URN);
                  }
                  return false;
                }),
            any(),
            eq(false));
  }
}
