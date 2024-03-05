package com.linkedin.datahub.graphql.resolvers.group;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityCountInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EntityCountsResolverTest {

  private static final String VIEW_URN = "urn:li:dataHubView:81921c81-4e26-4a2e-a47b-33c60183461s";
  private EntityCountInput _input;
  private ViewService _viewService;
  private EntityClient _entityClient;
  private EntityCountsResolver _resolver;
  private Authentication _authentication;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _viewService = mock(ViewService.class);
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);
    _input = new EntityCountInput();
    _input.setViewUrn(VIEW_URN);
    _input.setTypes(List.of(EntityType.DATASET, EntityType.DATA_PLATFORM));

    _resolver = new EntityCountsResolver(_entityClient, _viewService);
  }

  @Test
  public void testPasses() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(_input);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    _resolver.get(_dataFetchingEnvironment).join();
  }
}
