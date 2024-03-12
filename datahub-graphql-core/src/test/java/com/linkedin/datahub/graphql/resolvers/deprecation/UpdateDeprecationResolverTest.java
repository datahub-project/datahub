package com.linkedin.datahub.graphql.resolvers.deprecation;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateDeprecationInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateDeprecationResolverTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final UpdateDeprecationInput TEST_DEPRECATION_INPUT =
      new UpdateDeprecationInput(TEST_ENTITY_URN, true, 0L, "Test note");
  private static final CorpuserUrn TEST_ACTOR_URN = new CorpuserUrn("test");

  @Test
  public void testGetSuccessNoExistingDeprecation() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.batchGetV2(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                Mockito.eq(ImmutableSet.of(Constants.DEPRECATION_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService mockService = getMockEntityService();
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    final Deprecation newDeprecation =
        new Deprecation()
            .setDeprecated(true)
            .setDecommissionTime(0L)
            .setNote("Test note")
            .setActor(TEST_ACTOR_URN);
    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            UrnUtils.getUrn(TEST_ENTITY_URN), DEPRECATION_ASPECT_NAME, newDeprecation);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(Mockito.eq(proposal), Mockito.any(Authentication.class), Mockito.eq(false));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)));
  }

  @Test
  public void testGetSuccessExistingDeprecation() throws Exception {
    Deprecation originalDeprecation =
        new Deprecation()
            .setDeprecated(false)
            .setDecommissionTime(1L)
            .setActor(TEST_ACTOR_URN)
            .setNote("");

    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.batchGetV2(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                Mockito.eq(ImmutableSet.of(Constants.DEPRECATION_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DEPRECATION_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(originalDeprecation.data())))))));

    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    final Deprecation newDeprecation =
        new Deprecation()
            .setDeprecated(true)
            .setDecommissionTime(0L)
            .setNote("Test note")
            .setActor(TEST_ACTOR_URN);
    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            UrnUtils.getUrn(TEST_ENTITY_URN), DEPRECATION_ASPECT_NAME, newDeprecation);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(Mockito.eq(proposal), Mockito.any(Authentication.class), Mockito.eq(false));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)));
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.batchGetV2(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                Mockito.eq(ImmutableSet.of(Constants.DEPRECATION_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DEPRECATION_ASPECT_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(false);

    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(Mockito.any(), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = Mockito.mock(EntityService.class);
    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(Mockito.any(), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(Mockito.any(), Mockito.any(Authentication.class));
    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
