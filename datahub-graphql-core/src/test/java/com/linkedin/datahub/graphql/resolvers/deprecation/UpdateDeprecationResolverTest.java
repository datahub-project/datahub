package com.linkedin.datahub.graphql.resolvers.deprecation;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

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

  // Create SchemaField URN for the entity being deprecated
  private static final String TEST_SCHEMA_FIELD_URN =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD),old_user_id)";

  // Create SchemaField URN for the replacement
  private static final String REPLACEMENT_SCHEMA_FIELD_URN =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD),new_user_id)";

  private static final UpdateDeprecationInput TEST_DEPRECATION_INPUT =
      new UpdateDeprecationInput(TEST_ENTITY_URN, null, null, true, 0L, "Test note", null);

  private static final UpdateDeprecationInput TEST_DEPRECATION_INPUT_WITH_REPLACEMENT =
      new UpdateDeprecationInput(
          TEST_ENTITY_URN,
          null,
          null,
          true,
          0L,
          "Test note",
          "urn:li:dataset:(urn:li:dataPlatform:mysql,replacement-dataset,PROD)");
  private static final CorpuserUrn TEST_ACTOR_URN = new CorpuserUrn("test");

  @Test
  public void testGetSuccessNoExistingDeprecation() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                eq(ImmutableSet.of(Constants.DEPRECATION_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);

    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
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

    verifyIngestProposal(mockClient, 1, proposal);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true));
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
                any(),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                eq(ImmutableSet.of(Constants.DEPRECATION_ASPECT_NAME))))
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

    EntityService<?> mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);

    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
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

    verifyIngestProposal(mockClient, 1, proposal);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true));
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                eq(ImmutableSet.of(Constants.DEPRECATION_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DEPRECATION_ASPECT_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService<?> mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(false);

    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(any(), Mockito.any(), anyBoolean());
    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(TEST_DEPRECATION_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetSuccessNoExistingDeprecationWithReplacement() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                eq(ImmutableSet.of(Constants.DEPRECATION_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);

    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input")))
        .thenReturn(TEST_DEPRECATION_INPUT_WITH_REPLACEMENT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    final Deprecation newDeprecation =
        new Deprecation()
            .setDeprecated(true)
            .setDecommissionTime(0L)
            .setNote("Test note")
            .setReplacement(
                Urn.createFromString(
                    "urn:li:dataset:(urn:li:dataPlatform:mysql,replacement-dataset,PROD)"))
            .setActor(TEST_ACTOR_URN);
    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            UrnUtils.getUrn(TEST_ENTITY_URN), DEPRECATION_ASPECT_NAME, newDeprecation);

    verifyIngestProposal(mockClient, 1, proposal);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true));
  }

  @Test
  public void testGetSuccessSchemaFieldDeprecation() throws Exception {
    // Create test input using schema field URNs
    UpdateDeprecationInput testSchemaFieldInput =
        new UpdateDeprecationInput(
            TEST_SCHEMA_FIELD_URN,
            null,
            null,
            true,
            0L,
            "Deprecating old_user_id in favor of new_user_id",
            REPLACEMENT_SCHEMA_FIELD_URN);

    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                eq(Constants.SCHEMA_FIELD_ENTITY_NAME),
                eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_SCHEMA_FIELD_URN)))),
                eq(ImmutableSet.of(Constants.DEPRECATION_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_SCHEMA_FIELD_URN),
                new EntityResponse()
                    .setEntityName(Constants.SCHEMA_FIELD_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_SCHEMA_FIELD_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(
            mockService.exists(any(), eq(Urn.createFromString(TEST_SCHEMA_FIELD_URN)), eq(true)))
        .thenReturn(true);

    UpdateDeprecationResolver resolver = new UpdateDeprecationResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(testSchemaFieldInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Execute the resolver
    resolver.get(mockEnv).get();

    // Create expected deprecation object
    final Deprecation expectedDeprecation =
        new Deprecation()
            .setDeprecated(true)
            .setDecommissionTime(0L)
            .setNote("Deprecating old_user_id in favor of new_user_id")
            .setReplacement(Urn.createFromString(REPLACEMENT_SCHEMA_FIELD_URN))
            .setActor(TEST_ACTOR_URN);

    // Verify the correct proposal was made
    final MetadataChangeProposal expectedProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            UrnUtils.getUrn(TEST_SCHEMA_FIELD_URN), DEPRECATION_ASPECT_NAME, expectedDeprecation);

    verifyIngestProposal(mockClient, 1, expectedProposal);

    // Verify that existence check was performed
    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_SCHEMA_FIELD_URN)), eq(true));
  }
}
