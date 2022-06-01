package com.linkedin.datahub.graphql.resolvers.domain;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateDomainInput;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DomainKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class CreateDomainResolverTest {

  private static final CreateDomainInput TEST_INPUT = new CreateDomainInput(
      "test-id",
      "test-name",
      "test-description"
  );

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    final DomainKey key = new DomainKey();
    key.setId("test-id");
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.DOMAIN_ENTITY_NAME);
    DomainProperties props = new DomainProperties();
    props.setDescription("test-description");
    props.setName("test-name");
    proposal.setAspectName(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
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
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient);

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
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}