package com.linkedin.datahub.graphql.resolvers.owner;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
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
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class BatchRemoveOwnersResolverTest {

  private static final String TEST_ENTITY_URN_1 = "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 = "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_OWNER_URN_1 = "urn:li:corpuser:test-id-1";
  private static final String TEST_OWNER_URN_2 = "urn:li:corpuser:test-id-2";

  @Test
  public void testGetSuccessNoExistingOwners() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);
    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_URN_2))).thenReturn(true);

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveOwnersInput input = new BatchRemoveOwnersInput(ImmutableList.of(
        TEST_OWNER_URN_1,
        TEST_OWNER_URN_2
    ), ImmutableList.of(
        new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
        new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(2)).ingestProposal(
        Mockito.any(), // Ownership has a dynamically generated timestamp
        Mockito.any(AuditStamp.class)
    );
  }

  @Test
  public void testGetSuccessExistingOwners() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    final Ownership oldOwners1 = new Ownership().setOwners(new OwnerArray(ImmutableList.of(
        new Owner().setOwner(Urn.createFromString(TEST_OWNER_URN_1)).setType(OwnershipType.TECHNICAL_OWNER)
    )));

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(oldOwners1);

    final Ownership oldOwners2 = new Ownership().setOwners(new OwnerArray(ImmutableList.of(
        new Owner().setOwner(Urn.createFromString(TEST_OWNER_URN_2)).setType(OwnershipType.TECHNICAL_OWNER)
    )));

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(oldOwners2);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_URN_2))).thenReturn(true);

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveOwnersInput input = new BatchRemoveOwnersInput(ImmutableList.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2
    ), ImmutableList.of(
        new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
        new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(2)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
    );
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);
    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(false);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_URN_1))).thenReturn(true);

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveOwnersInput input = new BatchRemoveOwnersInput(ImmutableList.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2
    ), ImmutableList.of(
        new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
        new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(AuditStamp.class));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveOwnersInput input = new BatchRemoveOwnersInput(ImmutableList.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2
    ), ImmutableList.of(
        new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
        new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(AuditStamp.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.doThrow(RuntimeException.class).when(mockService).ingestProposal(
        Mockito.any(),
        Mockito.any(AuditStamp.class));

    BatchRemoveOwnersResolver resolver = new BatchRemoveOwnersResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BatchRemoveOwnersInput input = new BatchRemoveOwnersInput(ImmutableList.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2
    ), ImmutableList.of(
        new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
        new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}