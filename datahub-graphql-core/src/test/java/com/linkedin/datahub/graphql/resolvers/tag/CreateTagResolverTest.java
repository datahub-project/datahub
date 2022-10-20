package com.linkedin.datahub.graphql.resolvers.tag;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateTagInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.tag.TagProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.TagKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class CreateTagResolverTest {

  private static final CreateTagInput TEST_INPUT = new CreateTagInput(
      "test-id",
      "test-name",
      "test-description"
  );

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityService mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.ingestProposal(Mockito.any(MetadataChangeProposal.class), Mockito.any(Authentication.class)))
        .thenReturn(String.format("urn:li:tag:%s", TEST_INPUT.getId()));
    CreateTagResolver resolver = new CreateTagResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    final TagKey key = new TagKey();
    key.setName("test-id");
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.TAG_ENTITY_NAME);
    TagProperties props = new TagProperties();
    props.setDescription("test-description");
    props.setName("test-name");
    proposal.setAspectName(Constants.TAG_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(props));
    proposal.setChangeType(ChangeType.UPSERT);

    // Not ideal to match against "any", but we don't know the auto-generated execution request id
    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityService mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    CreateTagResolver resolver = new CreateTagResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityService mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RuntimeException.class).when(mockClient).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
    CreateTagResolver resolver = new CreateTagResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}