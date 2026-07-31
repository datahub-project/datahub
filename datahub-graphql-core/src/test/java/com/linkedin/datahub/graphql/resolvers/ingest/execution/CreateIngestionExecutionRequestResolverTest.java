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
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import org.json.JSONObject;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateIngestionExecutionRequestResolverTest {

  private static final CreateIngestionExecutionRequestInput TEST_INPUT =
      new CreateIngestionExecutionRequestInput(TEST_INGESTION_SOURCE_URN.toString());

  /**
   * A matrix service backed by a {@link NoOpIngestionCliVersionMatrixSource} — what production
   * wires when no matrix backend is configured. Always returns an empty matrix, so resolution falls
   * through to {@code defaultCliVersion}.
   */
  private static IngestionCliVersionMatrixService disabledMatrixService() {
    return new IngestionCliVersionMatrixService(
        new NoOpIngestionCliVersionMatrixSource(), null, null);
  }

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
        new CreateIngestionExecutionRequestResolver(
            mockClient, ingestionConfiguration, disabledMatrixService());

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
        new CreateIngestionExecutionRequestResolver(
            mockClient, ingestionConfiguration, disabledMatrixService());

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  // ---------------------------------------------------------------------------
  // Version matrix tests — use a source with NO per-source version so the
  // resolver falls through to the matrix / default-cli-version path.
  // ---------------------------------------------------------------------------

  /**
   * When the version matrix has an entry for the connector type, that version should win over the
   * global defaultCliVersion.
   */
  @Test
  public void testVersionMatrixConnectorSpecificVersionUsed() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    mockBatchGetV2(mockClient, sourceWithoutVersion("snowflake"));

    IngestionConfiguration config = new IngestionConfiguration();
    config.setDefaultCliVersion("default-global");

    // Matrix maps "1.3.1.4" → { "snowflake": "matrix-snowflake-version" }
    IngestionCliVersionMatrixService matrixService =
        matrixServiceForConnector("snowflake", "matrix-snowflake-version", "1.3.1.4");

    CreateIngestionExecutionRequestResolver resolver =
        new CreateIngestionExecutionRequestResolver(mockClient, config, matrixService);

    String resolvedVersion = executeAndCaptureVersion(mockClient, resolver);
    assertEquals(resolvedVersion, "matrix-snowflake-version");
  }

  /**
   * When the matrix has no entry for the connector under the current server version, the resolver
   * falls back to the global {@code defaultCliVersion}. (Replaces the old {@code _default}
   * server-level fallback the previous schema offered — the new schema requires explicit
   * per-connector entries, and unknown connectors fall through to the application default.)
   */
  @Test
  public void testVersionMatrixConnectorNotPresent_fallsBackToDefaultCliVersion() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    mockBatchGetV2(mockClient, sourceWithoutVersion("mysql"));

    IngestionConfiguration config = new IngestionConfiguration();
    config.setDefaultCliVersion("default-global");

    // Matrix has snowflake only; mysql is absent.
    IngestionCliVersionMatrixService matrixService =
        matrixServiceForConnector("snowflake", "matrix-snowflake-version", "1.3.1.4");

    CreateIngestionExecutionRequestResolver resolver =
        new CreateIngestionExecutionRequestResolver(mockClient, config, matrixService);

    String resolvedVersion = executeAndCaptureVersion(mockClient, resolver);
    assertEquals(resolvedVersion, "default-global");
  }

  /** When the matrix is disabled (null URL), the global {@code defaultCliVersion} is used. */
  @Test
  public void testVersionMatrixMissFallsBackToDefaultCliVersion() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    mockBatchGetV2(mockClient, sourceWithoutVersion("mysql"));

    IngestionConfiguration config = new IngestionConfiguration();
    config.setDefaultCliVersion("default-global");

    // Matrix service backed by a NoOp source → always returns empty
    IngestionCliVersionMatrixService matrixService =
        new IngestionCliVersionMatrixService(
            new com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource(),
            "1.3.1.4",
            null);

    CreateIngestionExecutionRequestResolver resolver =
        new CreateIngestionExecutionRequestResolver(mockClient, config, matrixService);

    String resolvedVersion = executeAndCaptureVersion(mockClient, resolver);
    assertEquals(resolvedVersion, "default-global");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static DataHubIngestionSourceInfo sourceWithoutVersion(String connectorType) {
    DataHubIngestionSourceInfo info = new DataHubIngestionSourceInfo();
    info.setName("Test Source");
    info.setType(connectorType);
    info.setSchedule(
        new DataHubIngestionSourceSchedule().setTimezone("UTC").setInterval("* * * * *"));
    // Deliberately omit .setVersion() so version resolution falls through to the matrix
    info.setConfig(new DataHubIngestionSourceConfig().setRecipe("{}").setExecutorId("default"));
    return info;
  }

  private static void mockBatchGetV2(EntityClient mockClient, DataHubIngestionSourceInfo info)
      throws Exception {
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
                                new EnvelopedAspect().setValue(new Aspect(info.data())))))));
  }

  /**
   * Returns a matrix service pre-loaded with a single entry under the new nested schema:
   *
   * <pre>{@code
   * { "<serverVersion>": { "<connector>": { "_default": "<version>" } } }
   * }</pre>
   *
   * <p>deploymentId is left null since these tests don't exercise cohort matching.
   */
  private static IngestionCliVersionMatrixService matrixServiceForConnector(
      String connector, String version, String serverVersion) throws Exception {
    String json =
        String.format("{\"%s\":{\"%s\":{\"_default\":\"%s\"}}}", serverVersion, connector, version);

    com.sun.net.httpserver.HttpServer server =
        com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/matrix",
        exchange -> {
          byte[] body = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (java.io.OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));
    String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/matrix";

    com.linkedin.metadata.ingestion.HttpUrlIngestionCliVersionMatrixSource httpSource =
        new com.linkedin.metadata.ingestion.HttpUrlIngestionCliVersionMatrixSource(url, 3600);
    IngestionCliVersionMatrixService svc =
        new IngestionCliVersionMatrixService(httpSource, serverVersion, null);

    // Wait for the initial background fetch to complete
    for (int i = 0; i < 20; i++) {
      if (svc.resolveVersion(connector).isPresent()) {
        break;
      }
      Thread.sleep(100);
    }
    return svc;
  }

  /** Runs the resolver and returns the {@code version} value from the captured execution args. */
  private static String executeAndCaptureVersion(
      EntityClient mockClient, CreateIngestionExecutionRequestResolver resolver) throws Exception {
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient, Mockito.atLeastOnce())
        .ingestProposal(any(), captor.capture(), anyBoolean());

    String aspectJson = captor.getValue().getAspect().getValue().asString(StandardCharsets.UTF_8);
    return new JSONObject(aspectJson).getJSONObject("args").getString("version");
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
        new CreateIngestionExecutionRequestResolver(
            mockClient, ingestionConfiguration, disabledMatrixService());

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
        new CreateIngestionExecutionRequestResolver(
            mockClient, ingestionConfiguration, disabledMatrixService());

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
