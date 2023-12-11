package com.linkedin.datahub.graphql.resolvers.ai;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.datahubproject.integrations.model.SuggestedDescription;

import java.util.concurrent.Future;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class SuggestDescriptionResolverTest {

  private IntegrationsService service;
  private SuggestDescriptionResolver resolver;
  private DataFetchingEnvironment environment;

  @BeforeMethod
  public void setUp() {
    service = Mockito.mock(IntegrationsService.class);
    resolver = new SuggestDescriptionResolver(service);
    environment = Mockito.mock(DataFetchingEnvironment.class);
  }

  @Test
  public void testGet() throws Exception {
    // Mock inputs
    final Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,TestDataset,PROD)");
    final String mockDescription = "test generated description";
    final SuggestedDescription suggestedDescription = new SuggestedDescription();
    suggestedDescription.setEntityDescription(mockDescription);

    // Mock environment and service behaviors
    QueryContext mockContext = getMockAllowContext();
    when(environment.getArgument("urn")).thenReturn(urn.toString());
    when(service.suggestDescription(urn)).thenReturn(suggestedDescription);
    when(environment.getContext()).thenReturn(mockContext);

    // Execute
    Future<String> future = resolver.get(environment);
    var result = future.get();

    // Verify
    assertEquals(result, mockDescription);
  }
}