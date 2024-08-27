package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateTestConnectionRequestInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateTestConnectionRequestResolverTest {

  private static final CreateTestConnectionRequestInput TEST_INPUT =
      new CreateTestConnectionRequestInput("{}", "0.8.44");

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion("default");
    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfiguration);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(any(), Mockito.any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion("default");
    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfiguration);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }
}
