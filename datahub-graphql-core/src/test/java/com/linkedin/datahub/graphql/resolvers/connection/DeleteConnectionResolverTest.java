package com.linkedin.datahub.graphql.resolvers.connection;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DeleteDataHubConnectionInput;
import com.linkedin.metadata.connection.ConnectionService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteConnectionResolverTest {

  private ConnectionService connectionService;
  private DeleteConnectionResolver resolver;

  @BeforeMethod
  public void setUp() {
    connectionService = Mockito.mock(ConnectionService.class);
    resolver = new DeleteConnectionResolver(connectionService);
  }

  @Test
  public void testGetAuthorized() throws Exception {
    // Mock inputs
    Urn connectionUrn = UrnUtils.getUrn("urn:li:dataHubConnection:test-id");

    final DeleteDataHubConnectionInput input = new DeleteDataHubConnectionInput();
    input.setUrn(connectionUrn.toString());

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    when(connectionService.softDeleteConnection(
            any(OperationContext.class), Mockito.eq(connectionUrn)))
        .thenReturn(true);

    final Boolean success = resolver.get(mockEnv).get();

    Assert.assertTrue(success);

    Mockito.verify(connectionService, Mockito.times(1))
        .softDeleteConnection(any(OperationContext.class), Mockito.eq(connectionUrn));
  }

  @Test
  public void testGetUnAuthorized() {
    // Mock inputs
    Urn connectionUrn = UrnUtils.getUrn("urn:li:dataHubConnection:test-id");

    final DeleteDataHubConnectionInput input = new DeleteDataHubConnectionInput();
    input.setUrn(connectionUrn.toString());

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
