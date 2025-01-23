package com.linkedin.datahub.graphql.resolvers.owner;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchRemoveOwnersInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchRemoveOwnersResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchRemoveOwnersResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_OWNER_URN_1 = "urn:li:corpuser:test-id-1";
  private static final String TEST_OWNER_URN_2 = "urn:li:corpuser:test-id-2";

  @Test
  public void testGetSuccessNoExistingOwners() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                eq(Constants.OWNERSHIP_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                eq(Constants.OWNERSHIP_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_URN_2)), eq(true)))
        .thenReturn(true);

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveOwnersInput input =
        new BatchRemoveOwnersInput(
            ImmutableList.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2),
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetSuccessExistingOwners() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    final Ownership oldOwners1 =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    ImmutableList.of(
                        new Owner()
                            .setOwner(Urn.createFromString(TEST_OWNER_URN_1))
                            .setType(OwnershipType.TECHNICAL_OWNER))));

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                eq(Constants.OWNERSHIP_ASPECT_NAME),
                eq(0L)))
        .thenReturn(oldOwners1);

    final Ownership oldOwners2 =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    ImmutableList.of(
                        new Owner()
                            .setOwner(Urn.createFromString(TEST_OWNER_URN_2))
                            .setType(OwnershipType.TECHNICAL_OWNER))));

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                eq(Constants.OWNERSHIP_ASPECT_NAME),
                eq(0L)))
        .thenReturn(oldOwners2);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_URN_2)), eq(true)))
        .thenReturn(true);

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveOwnersInput input =
        new BatchRemoveOwnersInput(
            ImmutableList.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2),
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                eq(Constants.OWNERSHIP_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                eq(Constants.OWNERSHIP_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_URN_1)), eq(true)))
        .thenReturn(true);

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveOwnersInput input =
        new BatchRemoveOwnersInput(
            ImmutableList.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2),
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService, mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveOwnersInput input =
        new BatchRemoveOwnersInput(
            ImmutableList.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2),
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService, mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BatchRemoveOwnersInput input =
        new BatchRemoveOwnersInput(
            ImmutableList.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2),
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
