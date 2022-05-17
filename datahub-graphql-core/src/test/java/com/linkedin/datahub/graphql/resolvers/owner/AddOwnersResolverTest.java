package com.linkedin.datahub.graphql.resolvers.owner;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AddOwnersInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.resolvers.mutate.AddOwnersResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class AddOwnersResolverTest {

  private static final String TEST_ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_OWNER_1_URN = "urn:li:corpuser:test-id-1";
  private static final String TEST_OWNER_2_URN = "urn:li:corpuser:test-id-2";

  @Test
  public void testGetSuccessNoExistingOwners() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_2_URN))).thenReturn(true);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddOwnersInput input = new AddOwnersInput(ImmutableList.of(
        new OwnerInput(TEST_OWNER_1_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER),
        new OwnerInput(TEST_OWNER_2_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER)
    ), TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    // Unable to easily validate exact payload due to the injected timestamp
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_OWNER_1_URN))
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_OWNER_2_URN))
    );
  }

  @Test
  public void testGetSuccessExistingOwners() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_2_URN))).thenReturn(true);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddOwnersInput input = new AddOwnersInput(ImmutableList.of(
        new OwnerInput(TEST_OWNER_1_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER),
        new OwnerInput(TEST_OWNER_2_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER)
    ), TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    // Unable to easily validate exact payload due to the injected timestamp
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_OWNER_1_URN))
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_OWNER_2_URN))
    );
  }

  @Test
  public void testGetFailureOwnerDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_1_URN))).thenReturn(false);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddOwnersInput input = new AddOwnersInput(ImmutableList.of(
        new OwnerInput(TEST_OWNER_1_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER)), TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(AuditStamp.class));
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
        Mockito.eq(Constants.OWNERSHIP_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(false);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_OWNER_1_URN))).thenReturn(true);

    AddOwnersResolver resolver = new AddOwnersResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddOwnersInput input = new AddOwnersInput(ImmutableList.of(
        new OwnerInput(TEST_OWNER_1_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER)), TEST_ENTITY_URN);
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

    AddOwnersResolver resolver = new AddOwnersResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddOwnersInput input = new AddOwnersInput(ImmutableList.of(
        new OwnerInput(TEST_OWNER_1_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER)), TEST_ENTITY_URN);
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

    AddOwnersResolver resolver = new AddOwnersResolver(Mockito.mock(EntityService.class));

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    AddOwnersInput input = new AddOwnersInput(ImmutableList.of(
        new OwnerInput(TEST_OWNER_1_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER)), TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}