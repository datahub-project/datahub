package com.linkedin.datahub.graphql.resolvers.ai;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.InferredColumnDescriptions;
import com.linkedin.datahub.graphql.generated.InferredDocumentation;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.invoker.JSON;
import io.datahubproject.integrations.model.SuggestedDescription;
import java.util.Map;
import java.util.concurrent.Future;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InferDocumentationResolverTest {

  private IntegrationsService service;
  private InferDocumentationResolver resolver;
  private DataFetchingEnvironment environment;

  @BeforeMethod
  public void setUp() {
    service = Mockito.mock(IntegrationsService.class);
    resolver = new InferDocumentationResolver(service);
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
    when(service.inferDocumentation(urn)).thenReturn(suggestedDescription);
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
}
