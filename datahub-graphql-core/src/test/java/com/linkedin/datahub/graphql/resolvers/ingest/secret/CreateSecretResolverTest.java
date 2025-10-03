package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateSecretInput;
import com.linkedin.datahub.graphql.resolvers.ingest.source.UpsertIngestionSourceResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataHubSecretKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateSecretResolverTest {

  private static final CreateSecretInput TEST_INPUT =
      new CreateSecretInput("MY_SECRET", "mysecretvalue", "none");

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);
    Mockito.when(mockSecretService.encrypt(Mockito.eq(TEST_INPUT.getValue())))
        .thenReturn("encryptedvalue");
    CreateSecretResolver resolver = new CreateSecretResolver(mockClient, mockSecretService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Invoke the resolver
    resolver.get(mockEnv).join();

    // Verify ingest proposal has been called
    DataHubSecretKey key = new DataHubSecretKey();
    key.setId(TEST_INPUT.getName());

    DataHubSecretValue value = new DataHubSecretValue();
    value.setValue("encryptedvalue");
    value.setName(TEST_INPUT.getName());
    value.setDescription(TEST_INPUT.getDescription());
    value.setCreated(
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:test")).setTime(0L));

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(
                new CreateSecretResolverMatcherTest(
                    new MetadataChangeProposal()
                        .setChangeType(ChangeType.UPSERT)
                        .setEntityType(Constants.SECRETS_ENTITY_NAME)
                        .setAspectName(Constants.SECRET_VALUE_ASPECT_NAME)
                        .setAspect(GenericRecordUtils.serializeAspect(value))
                        .setEntityKeyAspect(GenericRecordUtils.serializeAspect(key)))),
            Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    UpsertIngestionSourceResolver resolver = new UpsertIngestionSourceResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(any(), Mockito.any(), anyBoolean());
    UpsertIngestionSourceResolver resolver = new UpsertIngestionSourceResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}
