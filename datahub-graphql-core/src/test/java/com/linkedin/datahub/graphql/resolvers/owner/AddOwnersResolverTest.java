package com.linkedin.datahub.graphql.resolvers.owner;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AddOwnersInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.resolvers.mutate.AddOwnersResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AddOwnersResolverTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_OWNER_1_URN = "urn:li:corpuser:test-id-1";
  private static final String TEST_OWNER_2_URN = "urn:li:corpuser:test-id-2";
  private static final String TEST_OWNER_3_URN = "urn:li:corpGroup:test-id-3";

  @Test
  public void testGetSuccessNoExistingOwners() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_2_URN)), eq(true)))
        .thenReturn(true);

    Mockito.when(
            mockService.exists(
                any(),
                eq(
                    Urn.createFromString(
                        OwnerUtils.mapOwnershipTypeToEntity(
                            com.linkedin.datahub.graphql.generated.OwnershipType.TECHNICAL_OWNER
                                .name()))),
                eq(true)))
        .thenReturn(true);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddOwnersInput input =
        new AddOwnersInput(
            ImmutableList.of(
                new OwnerInput(
                    TEST_OWNER_1_URN,
                    OwnerEntityType.CORP_USER,
                    OwnershipType.TECHNICAL_OWNER,
                    OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name())),
                new OwnerInput(
                    TEST_OWNER_2_URN,
                    OwnerEntityType.CORP_USER,
                    OwnershipType.TECHNICAL_OWNER,
                    OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name()))),
            TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    // Unable to easily validate exact payload due to the injected timestamp
    verifyIngestProposal(mockService, 1);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_OWNER_1_URN)), eq(true));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_OWNER_2_URN)), eq(true));
  }

  @Test
  public void testGetSuccessExistingOwnerNewType() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    com.linkedin.common.Ownership oldOwnership =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    ImmutableList.of(
                        new Owner()
                            .setOwner(UrnUtils.getUrn(TEST_OWNER_1_URN))
                            .setType(com.linkedin.common.OwnershipType.NONE)
                            .setSource(
                                new OwnershipSource().setType(OwnershipSourceType.MANUAL)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldOwnership);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_1_URN)), eq(true)))
        .thenReturn(true);

    Mockito.when(
            mockService.exists(
                any(),
                eq(
                    Urn.createFromString(
                        OwnerUtils.mapOwnershipTypeToEntity(
                            com.linkedin.datahub.graphql.generated.OwnershipType.TECHNICAL_OWNER
                                .name()))),
                eq(true)))
        .thenReturn(true);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    AddOwnersInput input =
        new AddOwnersInput(
            ImmutableList.of(
                OwnerInput.builder()
                    .setOwnerUrn(TEST_OWNER_1_URN)
                    .setOwnershipTypeUrn(
                        OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name()))
                    .setOwnerEntityType(OwnerEntityType.CORP_USER)
                    .build()),
            TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    // Unable to easily validate exact payload due to the injected timestamp
    verifyIngestProposal(mockService, 1);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_OWNER_1_URN)), eq(true));
  }

  @Test
  public void testGetSuccessDeprecatedTypeToOwnershipType() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    com.linkedin.common.Ownership oldOwnership =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    ImmutableList.of(
                        new Owner()
                            .setOwner(UrnUtils.getUrn(TEST_OWNER_1_URN))
                            .setType(com.linkedin.common.OwnershipType.TECHNICAL_OWNER)
                            .setSource(
                                new OwnershipSource().setType(OwnershipSourceType.MANUAL)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldOwnership);

    Mockito.when(mockService.exists(any(), any(Urn.class), eq(true))).thenReturn(true);

    Mockito.when(
            mockService.exists(
                any(),
                eq(
                    Urn.createFromString(
                        OwnerUtils.mapOwnershipTypeToEntity(
                            com.linkedin.datahub.graphql.generated.OwnershipType.TECHNICAL_OWNER
                                .name()))),
                eq(true)))
        .thenReturn(true);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    AddOwnersInput input =
        new AddOwnersInput(
            ImmutableList.of(
                OwnerInput.builder()
                    .setOwnerUrn(TEST_OWNER_1_URN)
                    .setOwnershipTypeUrn(
                        OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name()))
                    .setOwnerEntityType(OwnerEntityType.CORP_USER)
                    .build()),
            TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    // Unable to easily validate exact payload due to the injected timestamp
    verifyIngestProposal(mockService, 1);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_OWNER_1_URN)), eq(true));
  }

  @Test
  public void testGetSuccessMultipleOwnerTypes() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    com.linkedin.common.Ownership oldOwnership =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    ImmutableList.of(
                        new Owner()
                            .setOwner(UrnUtils.getUrn(TEST_OWNER_1_URN))
                            .setType(com.linkedin.common.OwnershipType.NONE)
                            .setSource(
                                new OwnershipSource().setType(OwnershipSourceType.MANUAL)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldOwnership);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_2_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_3_URN)), eq(true)))
        .thenReturn(true);

    Mockito.when(
            mockService.exists(
                any(),
                eq(
                    Urn.createFromString(
                        OwnerUtils.mapOwnershipTypeToEntity(
                            com.linkedin.datahub.graphql.generated.OwnershipType.TECHNICAL_OWNER
                                .name()))),
                eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(),
                eq(
                    Urn.createFromString(
                        OwnerUtils.mapOwnershipTypeToEntity(
                            com.linkedin.datahub.graphql.generated.OwnershipType.BUSINESS_OWNER
                                .name()))),
                eq(true)))
        .thenReturn(true);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    AddOwnersInput input =
        new AddOwnersInput(
            ImmutableList.of(
                OwnerInput.builder()
                    .setOwnerUrn(TEST_OWNER_1_URN)
                    .setOwnershipTypeUrn(
                        OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name()))
                    .setOwnerEntityType(OwnerEntityType.CORP_USER)
                    .build(),
                OwnerInput.builder()
                    .setOwnerUrn(TEST_OWNER_2_URN)
                    .setOwnershipTypeUrn(
                        OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.BUSINESS_OWNER.name()))
                    .setOwnerEntityType(OwnerEntityType.CORP_USER)
                    .build(),
                OwnerInput.builder()
                    .setOwnerUrn(TEST_OWNER_3_URN)
                    .setOwnershipTypeUrn(
                        OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name()))
                    .setOwnerEntityType(OwnerEntityType.CORP_GROUP)
                    .build()),
            TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    // Unable to easily validate exact payload due to the injected timestamp
    verifyIngestProposal(mockService, 1);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_OWNER_1_URN)), eq(true));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_OWNER_2_URN)), eq(true));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_OWNER_3_URN)), eq(true));
  }

  @Test
  public void testGetFailureOwnerDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_1_URN)), eq(true)))
        .thenReturn(false);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddOwnersInput input =
        new AddOwnersInput(
            ImmutableList.of(
                new OwnerInput(
                    TEST_OWNER_1_URN,
                    OwnerEntityType.CORP_USER,
                    OwnershipType.TECHNICAL_OWNER,
                    OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name()))),
            TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_OWNER_1_URN)), eq(true)))
        .thenReturn(true);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddOwnersInput input =
        new AddOwnersInput(
            ImmutableList.of(
                new OwnerInput(
                    TEST_OWNER_1_URN,
                    OwnerEntityType.CORP_USER,
                    OwnershipType.TECHNICAL_OWNER,
                    OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name()))),
            TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService, mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddOwnersInput input =
        new AddOwnersInput(
            ImmutableList.of(
                new OwnerInput(
                    TEST_OWNER_1_URN,
                    OwnerEntityType.CORP_USER,
                    OwnershipType.TECHNICAL_OWNER,
                    OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name()))),
            TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
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
        .ingestProposal(any(), any(AspectsBatchImpl.class), Mockito.anyBoolean());

    AddOwnersResolver resolver =
        new AddOwnersResolver(Mockito.mock(EntityService.class), mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    AddOwnersInput input =
        new AddOwnersInput(
            ImmutableList.of(
                new OwnerInput(
                    TEST_OWNER_1_URN,
                    OwnerEntityType.CORP_USER,
                    OwnershipType.TECHNICAL_OWNER,
                    OwnerUtils.mapOwnershipTypeToEntity(OwnershipType.TECHNICAL_OWNER.name()))),
            TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
