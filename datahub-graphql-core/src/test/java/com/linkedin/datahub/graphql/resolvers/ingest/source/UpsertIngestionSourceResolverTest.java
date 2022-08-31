package com.linkedin.datahub.graphql.resolvers.ingest.source;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceConfigInput;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceInput;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceScheduleInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.testng.Assert.*;


public class UpsertIngestionSourceResolverTest {

  private static final UpdateIngestionSourceInput TEST_INPUT = new UpdateIngestionSourceInput(
      "Test source",
      "mysql", "Test source description",
      new UpdateIngestionSourceScheduleInput("* * * * *", "UTC"),
      new UpdateIngestionSourceConfigInput("my test recipe", "0.8.18", "executor id", false)
  );

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    UpsertIngestionSourceResolver resolver = new UpsertIngestionSourceResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).join();

    // Verify ingest proposal has been called
    DataHubIngestionSourceInfo info = new DataHubIngestionSourceInfo();
    info.setType(TEST_INPUT.getType());
    info.setName(TEST_INPUT.getName());
    info.setSchedule(new DataHubIngestionSourceSchedule()
        .setInterval(TEST_INPUT.getSchedule().getInterval())
        .setTimezone(TEST_INPUT.getSchedule().getTimezone())
    );
    info.setConfig(new DataHubIngestionSourceConfig()
        .setRecipe(TEST_INPUT.getConfig().getRecipe())
        .setVersion(TEST_INPUT.getConfig().getVersion())
        .setExecutorId(TEST_INPUT.getConfig().getExecutorId())
        .setDebugMode(TEST_INPUT.getConfig().getDebugMode())
    );

    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(
            new MetadataChangeProposal()
              .setChangeType(ChangeType.UPSERT)
              .setEntityType(Constants.INGESTION_SOURCE_ENTITY_NAME)
              .setAspectName(Constants.INGESTION_INFO_ASPECT_NAME)
              .setAspect(GenericRecordUtils.serializeAspect(info))
              .setEntityUrn(TEST_INGESTION_SOURCE_URN)
        ),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    UpsertIngestionSourceResolver resolver = new UpsertIngestionSourceResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(
        TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
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
    UpsertIngestionSourceResolver resolver = new UpsertIngestionSourceResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}
