package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockEntityService;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteGlossaryEntityResolverTest {

  private static final String TEST_TERM_URN =
      "urn:li:glossaryTerm:12372c2ec7754c308993202dc44f548b";

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = getMockEntityService();

    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_URN))).thenReturn(true);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_TERM_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DeleteGlossaryEntityResolver resolver =
        new DeleteGlossaryEntityResolver(mockClient, mockService);
    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(
            Mockito.eq(Urn.createFromString(TEST_TERM_URN)), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .deleteEntity(Mockito.any(), Mockito.any(Authentication.class));

    EntityService mockService = getMockEntityService();
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_URN))).thenReturn(true);

    DeleteGlossaryEntityResolver resolver =
        new DeleteGlossaryEntityResolver(mockClient, mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_TERM_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
