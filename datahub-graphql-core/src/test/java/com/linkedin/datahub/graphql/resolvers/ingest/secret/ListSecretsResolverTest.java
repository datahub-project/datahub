package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListSecretsInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.testng.Assert.*;


public class ListSecretsResolverTest {

  private static final ListSecretsInput TEST_INPUT = new ListSecretsInput(
      0, 20, null
  );

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    DataHubSecretValue returnedValue = getTestSecretValue();

    Mockito.when(mockClient.search(
        Mockito.eq(Constants.SECRETS_ENTITY_NAME),
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
            .setEntities(new SearchEntityArray(ImmutableSet.of(new SearchEntity().setEntity(TEST_SECRET_URN))))
    );

    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(Constants.SECRETS_ENTITY_NAME),
        Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_SECRET_URN))),
        Mockito.eq(ImmutableSet.of(Constants.SECRET_VALUE_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    )).thenReturn(
        ImmutableMap.of(
            TEST_SECRET_URN,
            new EntityResponse()
                .setEntityName(Constants.SECRETS_ENTITY_NAME)
                .setUrn(TEST_SECRET_URN)
                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                    Constants.SECRET_VALUE_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(returnedValue.data()))
                )))
        )
    );
    ListSecretsResolver resolver = new ListSecretsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    assertEquals((int) resolver.get(mockEnv).get().getStart(), 0);
    assertEquals((int) resolver.get(mockEnv).get().getCount(), 1);
    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getSecrets().size(), 1);
    verifyTestSecretGraphQL(resolver.get(mockEnv).get().getSecrets().get(0), returnedValue);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListSecretsResolver resolver = new ListSecretsResolver(mockClient);

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
    ListSecretsResolver resolver = new ListSecretsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}
