package com.linkedin.datahub.graphql.resolvers.ai;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.InferredColumnDescriptions;
import com.linkedin.datahub.graphql.generated.InferredDocumentation;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.invoker.JSON;
import io.datahubproject.integrations.model.SuggestedDescription;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InferDocumentationResolverTest {

  private IntegrationsService service;
  private EntityClient client;
  private InferDocumentationResolver resolver;
  private DataFetchingEnvironment environment;

  @BeforeMethod
  public void setUp() {
    service = Mockito.mock(IntegrationsService.class);
    client = Mockito.mock(EntityClient.class);
    resolver = new InferDocumentationResolver(service, client);
    environment = Mockito.mock(DataFetchingEnvironment.class);
  }

  @Test
  public void testGet() throws Exception {
    // Mock inputs
    final Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,TestDataset,PROD)");
    final String mockDescription = "test generated description";
    final Map<String, String> mockColumnDescriptions = Map.of("id", "Silly little id column");
    final SuggestedDescription suggestedDescription = new SuggestedDescription();
    suggestedDescription.setEntityDescription(mockDescription);
    suggestedDescription.setColumnDescriptions(mockColumnDescriptions);

    final InferredDocumentation inferredDescription = new InferredDocumentation();
    inferredDescription.setEntityDescription(mockDescription);
    final InferredColumnDescriptions inferredColumnDescriptions = new InferredColumnDescriptions();
    inferredColumnDescriptions.setJsonBlob(JSON.serialize(mockColumnDescriptions));
    inferredDescription.setColumnDescriptions(inferredColumnDescriptions);

    // Mock environment and service behaviors
    QueryContext mockContext = getMockAllowContext();
    when(environment.getArgument("urn")).thenReturn(urn.toString());
    when(environment.getArgumentOrDefault("saveResult", false)).thenReturn(false);
    when(service.inferDocumentation(urn, mockContext.getActorUrn()))
        .thenReturn(CompletableFuture.completedFuture(suggestedDescription));
    when(environment.getContext()).thenReturn(mockContext);

    // Execute
    Future<InferredDocumentation> future = resolver.get(environment);
    var result = future.get();

    // Verify
    assertEquals(result.getEntityDescription(), inferredDescription.getEntityDescription());
    assertEquals(
        result.getColumnDescriptions().getJsonBlob(),
        inferredDescription.getColumnDescriptions().getJsonBlob());
  }

  @Test
  public void testGetAndSave() throws Exception {
    // Mock inputs
    final Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,TestDataset,PROD)");
    final String mockDescription = "test generated description";
    final String field1Path = "id";
    final Map<String, String> mockColumnDescriptions = Map.of(field1Path, "Silly little id column");
    final SuggestedDescription suggestedDescription = new SuggestedDescription();
    suggestedDescription.setEntityDescription(mockDescription);
    suggestedDescription.setColumnDescriptions(mockColumnDescriptions);

    final InferredDocumentation inferredDescription = new InferredDocumentation();
    inferredDescription.setEntityDescription(mockDescription);
    final InferredColumnDescriptions inferredColumnDescriptions = new InferredColumnDescriptions();
    inferredColumnDescriptions.setJsonBlob(JSON.serialize(mockColumnDescriptions));
    inferredDescription.setColumnDescriptions(inferredColumnDescriptions);

    // Mock environment and service behaviors
    QueryContext mockContext = getMockAllowContext();
    when(environment.getArgument("urn")).thenReturn(urn.toString());
    when(environment.getArgumentOrDefault("saveResult", false)).thenReturn(true);
    when(service.inferDocumentation(urn, mockContext.getActorUrn()))
        .thenReturn(CompletableFuture.completedFuture(suggestedDescription));
    when(environment.getContext()).thenReturn(mockContext);

    // Execute
    Future<InferredDocumentation> future = resolver.get(environment);
    var result = future.get();

    // Verify result mapping
    assertEquals(result.getEntityDescription(), inferredDescription.getEntityDescription());
    assertEquals(
        result.getColumnDescriptions().getJsonBlob(),
        inferredDescription.getColumnDescriptions().getJsonBlob());

    // Verify saving behavior
    Mockito.verify(client, Mockito.times(2))
        .getV2(
            eq(mockContext.getOperationContext()),
            any(String.class),
            any(Urn.class),
            eq(Set.of(Constants.DOCUMENTATION_ASPECT_NAME)));

    final MetadataAttribution attribution =
        new MetadataAttribution()
            .setTime(mockContext.getOperationContext().getAuditStamp().getTime())
            .setActor(mockContext.getOperationContext().getAuditStamp().getActor())
            .setSourceDetail(new StringMap(Map.of("inferred", "true")));

    // Verify entity description saved
    final DocumentationAssociationArray associations =
        new DocumentationAssociationArray(
            new DocumentationAssociation()
                .setDocumentation(mockDescription)
                .setAttribution(attribution));
    final Documentation documentation =
        new Documentation()
            .setLastModified(mockContext.getOperationContext().getAuditStamp())
            .setDocumentations(associations);

    // Verify schema field description saved
    final DocumentationAssociationArray schemaAssociations =
        new DocumentationAssociationArray(
            new DocumentationAssociation()
                .setDocumentation(mockDescription)
                .setAttribution(attribution));
    final Documentation schemaDoc =
        new Documentation()
            .setLastModified(mockContext.getOperationContext().getAuditStamp())
            .setDocumentations(schemaAssociations);

    // TODO: get the test working with exact eq on expected MCPs
    List<MetadataChangeProposal> expectedMCPs =
        List.of(
            AspectUtils.buildMetadataChangeProposal(
                urn, Constants.DOCUMENTATION_ASPECT_NAME, documentation),
            AspectUtils.buildMetadataChangeProposal(
                SchemaFieldUtils.generateSchemaFieldUrn(urn, field1Path),
                Constants.DOCUMENTATION_ASPECT_NAME,
                schemaDoc));
    Mockito.verify(client, Mockito.times(1))
        .batchIngestProposals(
            eq(mockContext.getOperationContext()), any(Collection.class), eq(false));
  }

  @Test
  public void testGetAndSaveWithExistingDocs() throws Exception {
    // TODO: test to make sure if there's already docs, the non-ai are not overwritten, but the AI
    // docs are
  }
}
