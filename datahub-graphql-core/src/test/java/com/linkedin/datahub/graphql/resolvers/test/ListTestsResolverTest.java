package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.TestUtils.*;
<<<<<<< HEAD
import static org.mockito.Mockito.*;
=======
>>>>>>> oss_master
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListTestsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ListTestsResolverTest {

  private static final Urn TEST_URN = Urn.createFromTuple("test", "test-id");

  private static final ListTestsInput TEST_INPUT = new ListTestsInput(0, 20, null);
<<<<<<< HEAD

  private EntityClient mockClient;
  private ListTestsResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private Authentication authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    mockClient = mock(EntityClient.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    authentication = mock(Authentication.class);
    resolver = new ListTestsResolver(mockClient);
  }

  @Test
  public void testGetSuccess() throws Exception {
=======

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

>>>>>>> oss_master
    Mockito.when(
            mockClient.search(
                Mockito.eq(Constants.TEST_ENTITY_NAME),
                Mockito.eq(""),
<<<<<<< HEAD
                Mockito.eq(null),
                any(SortCriterion.class),
=======
                Mockito.eq(Collections.emptyMap()),
>>>>>>> oss_master
                Mockito.eq(0),
                Mockito.eq(20),
                Mockito.any(Authentication.class),
                Mockito.eq(new SearchFlags().setFulltext(true))))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_URN)))));
<<<<<<< HEAD
=======

    ListTestsResolver resolver = new ListTestsResolver(mockClient);
>>>>>>> oss_master

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    assertEquals(resolver.get(mockEnv).get().getStart(), 0);
    assertEquals(resolver.get(mockEnv).get().getCount(), 1);
    assertEquals(resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getTests().size(), 1);
    assertEquals(resolver.get(mockEnv).get().getTests().get(0).getUrn(), TEST_URN.toString());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .search(
            Mockito.any(),
            Mockito.eq(""),
            Mockito.anyMap(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.any(Authentication.class),
            Mockito.eq(new SearchFlags().setFulltext(true)));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
<<<<<<< HEAD
=======
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
>>>>>>> oss_master
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .search(
            Mockito.any(),
            Mockito.eq(""),
            Mockito.anyMap(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.any(Authentication.class),
            Mockito.eq(new SearchFlags().setFulltext(true)));
    ListTestsResolver resolver = new ListTestsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
