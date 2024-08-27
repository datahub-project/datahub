package com.linkedin.datahub.graphql.resolvers.ownership;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListOwnershipTypesInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.OwnershipTypeKey;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.ownership.OwnershipTypeInfo;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListOwnershipTypesResolverTest {

  private static final Urn TEST_OWNERSHIP_TYPE_URN =
      Urn.createFromTuple(Constants.OWNERSHIP_TYPE_ENTITY_NAME, "test");

  private static final ListOwnershipTypesInput TEST_INPUT =
      new ListOwnershipTypesInput(0, 20, "", null);

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    OwnershipTypeInfo returnedInfo = getOwnershipTypeInfo();
    final OwnershipTypeKey key = new OwnershipTypeKey();
    key.setId("test");

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.OWNERSHIP_TYPE_ENTITY_NAME),
                Mockito.eq(""),
                Mockito.eq(null),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_OWNERSHIP_TYPE_URN)))));

    ListOwnershipTypesResolver resolver = new ListOwnershipTypesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    assertEquals(resolver.get(mockEnv).get().getStart(), 0);
    assertEquals(resolver.get(mockEnv).get().getCount(), 1);
    assertEquals(resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getOwnershipTypes().size(), 1);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListOwnershipTypesResolver resolver = new ListOwnershipTypesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    Mockito.verify(mockClient, Mockito.times(0))
        .search(
            any(),
            Mockito.any(),
            Mockito.eq(""),
            Mockito.anyMap(),
            Mockito.anyInt(),
            Mockito.anyInt());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    ListOwnershipTypesResolver resolver = new ListOwnershipTypesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  public static OwnershipTypeInfo getOwnershipTypeInfo() {
    OwnershipTypeInfo info = new OwnershipTypeInfo();
    info.setName("My test ownership type");
    info.setDescription("some description");
    return info;
  }
}
