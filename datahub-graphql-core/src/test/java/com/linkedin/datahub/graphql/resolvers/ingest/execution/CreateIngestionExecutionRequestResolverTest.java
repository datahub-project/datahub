package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateIngestionExecutionRequestInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateIngestionExecutionRequestResolverTest {

  private static final CreateIngestionExecutionRequestInput TEST_INPUT =
      new CreateIngestionExecutionRequestInput(TEST_INGESTION_SOURCE_URN.toString());

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_INGESTION_SOURCE_URN))),
                Mockito.eq(ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_INGESTION_SOURCE_URN,
                new EntityResponse()
                    .setEntityName(Constants.INGESTION_SOURCE_ENTITY_NAME)
                    .setUrn(TEST_INGESTION_SOURCE_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.INGESTION_INFO_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(getTestIngestionSourceInfo().data())))))));
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion("default");
    CreateIngestionExecutionRequestResolver resolver =
        new CreateIngestionExecutionRequestResolver(mockClient, ingestionConfiguration);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Not ideal to match against "any", but we don't know the auto-generated execution request id
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(any(), Mockito.any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion("default");
    CreateIngestionExecutionRequestResolver resolver =
        new CreateIngestionExecutionRequestResolver(mockClient, ingestionConfiguration);

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
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion("default");
    CreateIngestionExecutionRequestResolver resolver =
        new CreateIngestionExecutionRequestResolver(mockClient, ingestionConfiguration);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testVersionUsesExplicitVersion() throws Exception {
    String expectedVersion = "0.10.5";
    String capturedVersion = executeAndCaptureVersion(buildSourceInfoWithVersion(expectedVersion));
    assertEquals(capturedVersion, expectedVersion);
  }

  @Test
  public void testVersionFallsBackWhenEmpty() throws Exception {
    // This is the bug scenario: bootstrap YAML renders an empty string for version
    String capturedVersion = executeAndCaptureVersion(buildSourceInfoWithVersion(""));
    assertEquals(capturedVersion, DEFAULT_CLI_VERSION);
  }

  @Test
  public void testVersionFallsBackWhenNull() throws Exception {
    // Version field not set at all
    DataHubIngestionSourceInfo info = new DataHubIngestionSourceInfo();
    info.setName("My Test Source");
    info.setType("mysql");
    info.setConfig(new DataHubIngestionSourceConfig().setRecipe("{}").setExecutorId("executor id"));
    // Note: no setVersion call

    String capturedVersion = executeAndCaptureVersion(info);
    assertEquals(capturedVersion, DEFAULT_CLI_VERSION);
  }

  private static final String DEFAULT_CLI_VERSION = "default";

  private static DataHubIngestionSourceInfo buildSourceInfoWithVersion(String version) {
    DataHubIngestionSourceInfo info = new DataHubIngestionSourceInfo();
    info.setName("My Test Source");
    info.setType("mysql");
    info.setConfig(
        new DataHubIngestionSourceConfig()
            .setVersion(version)
            .setRecipe("{}")
            .setExecutorId("executor id"));
    return info;
  }

  /**
   * Executes the resolver with the given ingestion source info and captures the version argument
   * from the ingested MetadataChangeProposal.
   */
  private String executeAndCaptureVersion(DataHubIngestionSourceInfo sourceInfo) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_INGESTION_SOURCE_URN))),
                Mockito.eq(ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_INGESTION_SOURCE_URN,
                new EntityResponse()
                    .setEntityName(Constants.INGESTION_SOURCE_ENTITY_NAME)
                    .setUrn(TEST_INGESTION_SOURCE_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.INGESTION_INFO_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(sourceInfo.data())))))));

    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_CLI_VERSION);
    CreateIngestionExecutionRequestResolver resolver =
        new CreateIngestionExecutionRequestResolver(mockClient, ingestionConfiguration);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient).ingestProposal(any(), mcpCaptor.capture(), Mockito.eq(false));

    ExecutionRequestInput execInput =
        GenericRecordUtils.deserializeAspect(
            mcpCaptor.getValue().getAspect().getValue(),
            mcpCaptor.getValue().getAspect().getContentType(),
            ExecutionRequestInput.class);
    return execInput.getArgs().get("version");
  }
}
