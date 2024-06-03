package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteDomainResolverTest {

  private static final String TEST_URN = "urn:li:domain:test-id";

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Domain has 0 child domains
    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq("domain"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(1)))
        .thenReturn(new SearchResult().setNumEntities(0));

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(), Mockito.eq(Urn.createFromString(TEST_URN)));
  }

  @Test
  public void testDeleteWithChildDomains() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Domain has child domains
    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq("domain"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(1)))
        .thenReturn(new SearchResult().setNumEntities(1));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockClient, Mockito.times(0)).deleteEntity(any(), Mockito.any());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).deleteEntity(any(), Mockito.any());
  }
}
