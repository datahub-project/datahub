package com.linkedin.datahub.graphql.resolvers.ingest.source;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.testng.Assert.*;


public class ListIngestionSourceResolverTest {

  private static final ListIngestionSourcesInput TEST_INPUT = new ListIngestionSourcesInput(0, 20, null);

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    DataHubIngestionSourceInfo returnedInfo = getTestIngestionSourceInfo();

    Mockito.when(mockClient.search(
        Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
        Mockito.eq(""),
        Mockito.eq(Collections.emptyMap()),
        Mockito.eq(0),
        Mockito.eq(20),
        Mockito.any(Authentication.class)
    )).thenReturn(
        new SearchResult()
        .setFrom(0)
        .setPageSize(1)
        .setNumEntities(1)
        .setEntities(new SearchEntityArray(ImmutableSet.of(new SearchEntity().setEntity(TEST_INGESTION_SOURCE_URN))))
    );

    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
        Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_INGESTION_SOURCE_URN))),
        Mockito.eq(ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    )).thenReturn(
        ImmutableMap.of(
            TEST_INGESTION_SOURCE_URN,
            new EntityResponse()
                .setEntityName(Constants.INGESTION_SOURCE_ENTITY_NAME)
                .setUrn(TEST_INGESTION_SOURCE_URN)
                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                    Constants.INGESTION_INFO_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(returnedInfo.data()))
                )))
        )
    );
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    assertEquals(resolver.get(mockEnv).get().getStart(), 0);
    assertEquals(resolver.get(mockEnv).get().getCount(), 1);
    assertEquals(resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getIngestionSources().size(), 1);
    verifyTestIngestionSourceGraphQL(resolver.get(mockEnv).get().getIngestionSources().get(0), returnedInfo);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(
        TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).batchGetV2(
        Mockito.any(),
        Mockito.anySet(),
        Mockito.anySet(),
        Mockito.any(Authentication.class));
    Mockito.verify(mockClient, Mockito.times(0)).search(
        Mockito.any(),
        Mockito.eq(""),
        Mockito.anyMap(),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).batchGetV2(
        Mockito.any(),
        Mockito.anySet(),
        Mockito.anySet(),
        Mockito.any(Authentication.class));
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}
