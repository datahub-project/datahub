package com.linkedin.datahub.graphql.resolvers.tag;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
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
import com.linkedin.tag.TagProperties;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SetTagColorResolverTest {

  private static final String TEST_ENTITY_URN = "urn:li:tag:test-tag";
  private static final String TEST_COLOR_HEX = "#FFFFFF";

  @Test
  public void testGetSuccessExistingProperties() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();

    // Test setting the domain
    final TagProperties oldTagProperties = new TagProperties().setName("Test Tag");
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)),
                Mockito.eq(Constants.TAG_PROPERTIES_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldTagProperties);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);

    SetTagColorResolver resolver = new SetTagColorResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    final TagProperties newTagProperties =
        new TagProperties().setName("Test Tag").setColorHex(TEST_COLOR_HEX);
    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            UrnUtils.getUrn(TEST_ENTITY_URN), TAG_PROPERTIES_ASPECT_NAME, newTagProperties);

    verifyIngestProposal(mockClient, 1, proposal);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true));
  }

  @Test
  public void testGetFailureNoExistingProperties() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();

    // Test setting the domain
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)),
                Mockito.eq(Constants.TAG_PROPERTIES_ASPECT_NAME),
                Mockito.eq(0)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);

    SetTagColorResolver resolver = new SetTagColorResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetFailureTagDoesNotExist() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Test setting the domain
    final TagProperties oldTagProperties = new TagProperties().setName("Test Tag");
    final EnvelopedAspect oldTagPropertiesAspect =
        new EnvelopedAspect()
            .setName(Constants.TAG_PROPERTIES_ASPECT_NAME)
            .setValue(new Aspect(oldTagProperties.data()));
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.TAG_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                Mockito.eq(ImmutableSet.of(Constants.TAG_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.TAG_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.TAG_PROPERTIES_ASPECT_NAME, oldTagPropertiesAspect)))));

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(false);

    SetTagColorResolver resolver = new SetTagColorResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    SetTagColorResolver resolver = new SetTagColorResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(any(), Mockito.any(), anyBoolean());
    SetTagColorResolver resolver =
        new SetTagColorResolver(mockClient, Mockito.mock(EntityService.class));

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
