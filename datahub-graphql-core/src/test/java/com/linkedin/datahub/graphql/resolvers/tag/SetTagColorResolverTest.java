package com.linkedin.datahub.graphql.resolvers.tag;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.tag.TagProperties;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class SetTagColorResolverTest {

  private static final String TEST_ENTITY_URN = "urn:li:tag:test-tag";
  private static final String TEST_COLOR_HEX = "#FFFFFF";

  @Test
  public void testGetSuccessExistingProperties() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = Mockito.mock(EntityService.class);

    // Test setting the domain
    final TagProperties oldTagProperties = new TagProperties().setName("Test Tag");
    Mockito.when(mockService.getAspect(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)),
        Mockito.eq(Constants.TAG_PROPERTIES_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(oldTagProperties);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    SetTagColorResolver resolver = new SetTagColorResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    final TagProperties newTagProperties = new TagProperties().setName("Test Tag").setColorHex(TEST_COLOR_HEX);
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN));
    proposal.setEntityType(Constants.TAG_ENTITY_NAME);
    proposal.setAspectName(Constants.TAG_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newTagProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any(Authentication.class)
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN))
    );
  }

  @Test
  public void testGetFailureNoExistingProperties() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = Mockito.mock(EntityService.class);

    // Test setting the domain
    Mockito.when(mockService.getAspect(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)),
        Mockito.eq(Constants.TAG_PROPERTIES_ASPECT_NAME),
        Mockito.eq(0)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    SetTagColorResolver resolver = new SetTagColorResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetFailureTagDoesNotExist() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Test setting the domain
    final TagProperties oldTagProperties = new TagProperties().setName("Test Tag");
    final EnvelopedAspect oldTagPropertiesAspect = new EnvelopedAspect()
        .setName(Constants.TAG_PROPERTIES_ASPECT_NAME)
        .setValue(new Aspect(oldTagProperties.data()));
    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(Constants.TAG_ENTITY_NAME),
        Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
        Mockito.eq(ImmutableSet.of(Constants.TAG_PROPERTIES_ASPECT_NAME)),
        Mockito.any(Authentication.class)))
        .thenReturn(ImmutableMap.of(Urn.createFromString(TEST_ENTITY_URN),
            new EntityResponse()
                .setEntityName(Constants.TAG_ENTITY_NAME)
                .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                    Constants.TAG_PROPERTIES_ASPECT_NAME,
                    oldTagPropertiesAspect)))));

    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(false);

    SetTagColorResolver resolver = new SetTagColorResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = Mockito.mock(EntityService.class);
    SetTagColorResolver resolver = new SetTagColorResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
    SetTagColorResolver resolver = new SetTagColorResolver(mockClient, Mockito.mock(EntityService.class));

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("colorHex"))).thenReturn(TEST_COLOR_HEX);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}