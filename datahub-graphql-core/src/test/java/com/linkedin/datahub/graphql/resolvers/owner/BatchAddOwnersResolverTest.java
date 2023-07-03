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
import com.linkedin.datahub.graphql.generated.BatchAddOwnersInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchAddOwnersResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class BatchAddOwnersResolverTest {

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

    BatchAddOwnersResolver resolver = new BatchAddOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddOwnersInput input = new BatchAddOwnersInput(ImmutableList.of(new OwnerInput(
            TEST_OWNER_URN_1,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER),
        new OwnerInput(
            TEST_OWNER_URN_2,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER)),
        ImmutableList.of(
            new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
            new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 2);

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_OWNER_URN_1))
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_OWNER_URN_2))
    );
  }

  @Test
  public void testGetSuccessExistingOwners() throws Exception {
    final Ownership originalOwnership = new Ownership().setOwners(new OwnerArray(ImmutableList.of(
        new Owner().setOwner(Urn.createFromString(TEST_OWNER_URN_1)).setType(OwnershipType.TECHNICAL_OWNER)
    )));
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(originalOwnership);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(originalOwnership);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_URN_2))).thenReturn(true);

    BatchAddOwnersResolver resolver = new BatchAddOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddOwnersInput input = new BatchAddOwnersInput(ImmutableList.of(new OwnerInput(
        TEST_OWNER_URN_1,
        OwnerEntityType.CORP_USER,
        com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER),
        new OwnerInput(
            TEST_OWNER_URN_2,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER)),
        ImmutableList.of(
          new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
          new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 2);

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_OWNER_URN_1))
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_OWNER_URN_2))
    );
  }

  @Test
  public void testGetFailureOwnerDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_URN_1))).thenReturn(false);

    BatchAddOwnersResolver resolver = new BatchAddOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddOwnersInput input = new BatchAddOwnersInput(ImmutableList.of(new OwnerInput(
            TEST_OWNER_URN_1,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER),
        new OwnerInput(
            TEST_OWNER_URN_2,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER)),
        ImmutableList.of(
            new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
            new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
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

    BatchAddOwnersResolver resolver = new BatchAddOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddOwnersInput input = new BatchAddOwnersInput(ImmutableList.of(new OwnerInput(
            TEST_OWNER_URN_1,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER),
        new OwnerInput(
            TEST_OWNER_URN_2,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER)),
        ImmutableList.of(
            new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
            new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    BatchAddOwnersResolver resolver = new BatchAddOwnersResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddOwnersInput input = new BatchAddOwnersInput(ImmutableList.of(new OwnerInput(
            TEST_OWNER_URN_1,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER),
        new OwnerInput(
            TEST_OWNER_URN_2,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER)),
        ImmutableList.of(
            new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
            new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.doThrow(RuntimeException.class).when(mockService).ingestProposal(
        Mockito.any(),
        Mockito.any(AuditStamp.class), Mockito.anyBoolean());

    BatchAddOwnersResolver resolver = new BatchAddOwnersResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BatchAddOwnersInput input = new BatchAddOwnersInput(ImmutableList.of(new OwnerInput(
            TEST_OWNER_URN_1,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER),
        new OwnerInput(
            TEST_OWNER_URN_2,
            OwnerEntityType.CORP_USER,
            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER)),
        ImmutableList.of(
            new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
            new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}