package com.linkedin.datahub.graphql.resolvers.embed;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Embed;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateEmbedInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class UpdateEmbedResolverTest {

  private static final String TEST_ENTITY_URN = "urn:li:dashboard:(looker,1)";
  private static final String TEST_RENDER_URL = "https://www.google.com";
  private static final UpdateEmbedInput TEST_EMBED_INPUT = new UpdateEmbedInput(
      TEST_ENTITY_URN,
      TEST_RENDER_URL
  );
  private static final CorpuserUrn TEST_ACTOR_URN = new CorpuserUrn("test");

  @Test
  public void testGetSuccessNoExistingEmbed() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)),
        Mockito.eq(Constants.EMBED_ASPECT_NAME),
        Mockito.eq(0L))).thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    UpdateEmbedResolver resolver = new UpdateEmbedResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_EMBED_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    final Embed newEmbed = new Embed().setRenderUrl(TEST_RENDER_URL);
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN));
    proposal.setEntityType(Constants.DASHBOARD_ENTITY_NAME);
    proposal.setAspectName(Constants.EMBED_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newEmbed));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN))
    );
  }

  @Test
  public void testGetSuccessExistingEmbed() throws Exception {
    Embed originalEmbed = new Embed().setRenderUrl("https://otherurl.com");

    // Create resolver
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)),
        Mockito.eq(Constants.EMBED_ASPECT_NAME),
        Mockito.eq(0L))).thenReturn(originalEmbed);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    UpdateEmbedResolver resolver = new UpdateEmbedResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_EMBED_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    final Embed newEmbed = new Embed().setRenderUrl(TEST_RENDER_URL);
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN));
    proposal.setEntityType(Constants.DASHBOARD_ENTITY_NAME);
    proposal.setAspectName(Constants.EMBED_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newEmbed));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN))
    );
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
        Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
        Mockito.eq(ImmutableSet.of(Constants.EMBED_ASPECT_NAME)),
        Mockito.any(Authentication.class)))
        .thenReturn(ImmutableMap.of(Urn.createFromString(TEST_ENTITY_URN),
            new EntityResponse()
                .setEntityName(Constants.DASHBOARD_ENTITY_NAME)
                .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(false);

    UpdateEmbedResolver resolver = new UpdateEmbedResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_EMBED_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );;
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityService mockService = Mockito.mock(EntityService.class);
    UpdateEmbedResolver resolver = new UpdateEmbedResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_EMBED_INPUT);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
    UpdateEmbedResolver resolver = new UpdateEmbedResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_EMBED_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}