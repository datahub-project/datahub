package com.linkedin.datahub.graphql.resolvers.integration;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.graphql.generated.GetLinkPreviewInput;
import com.linkedin.datahub.graphql.generated.LinkPreview;
import com.linkedin.datahub.graphql.generated.LinkPreviewType;
import com.linkedin.link.LinkPreviewInfo;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.Future;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetLinkPreviewResolverTest {

  private IntegrationsService service;
  private GetLinkPreviewResolver resolver;
  private DataFetchingEnvironment environment;

  @BeforeMethod
  public void setUp() {
    service = Mockito.mock(IntegrationsService.class);
    resolver = new GetLinkPreviewResolver(service);
    environment = Mockito.mock(DataFetchingEnvironment.class);
  }

  @Test
  public void testGet() throws Exception {
    // Mock inputs
    final GetLinkPreviewInput input = new GetLinkPreviewInput();
    input.setUrl("http://test.com");
    final LinkPreviewInfo info = new LinkPreviewInfo();
    info.setType(com.linkedin.link.LinkPreviewType.SLACK_MESSAGE);
    info.setLastRefreshedMs(123456789L);
    info.setJson("{\"title\":\"test\"}");

    // Mock environment and service behaviors
    when(environment.getArgument("input")).thenReturn(input);
    when(service.getLinkPreview(input.getUrl())).thenReturn(info);

    // Execute
    Future<LinkPreview> future = resolver.get(environment);
    LinkPreview result = future.get();

    // Verify
    assertEquals(result.getType(), LinkPreviewType.SLACK_MESSAGE);
    assertEquals(result.getLastRefreshed(), (Long) 123456789L);
    assertEquals(result.getJson(), "{\"title\":\"test\"}");
  }
}
