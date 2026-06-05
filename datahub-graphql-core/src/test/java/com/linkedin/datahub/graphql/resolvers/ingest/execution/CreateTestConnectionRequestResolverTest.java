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
import java.nio.charset.StandardCharsets;
import org.json.JSONObject;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateTestConnectionRequestResolverTest {

  private static final String DEFAULT_CLI_VERSION = "default-cli-version";
  private static final CreateTestConnectionRequestInput TEST_INPUT =
      new CreateTestConnectionRequestInput("{}", "0.8.44");

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfig());

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
    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfig());

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  // ---------------------------------------------------------------------------
  // Version-resolution regression tests
  //
  // Each test maps to one row of the PR description's behavior table:
  //
  //   | input.version | Before this PR                       | After this PR     |
  //   | ------------- | ------------------------------------ | ----------------- |
  //   | "0.8.44"      | args.version = "0.8.44"              | unchanged         |
  //   | null          | args.version omitted; executor falls | args.version =   |
  //   |               | back to bundled CLI                  | defaultCliVersion |
  //   | ""            | args.version omitted                 | defaultCliVersion |
  //   | "   "         | args.version omitted                 | defaultCliVersion |
  //
  // Each test captures the persisted ExecutionRequestInput aspect and asserts
  // on args.version, locking in the contract that all four input shapes route
  // through IngestionUtils.resolveIngestionCliVersion (the helper introduced
  // by #17471 for the manual-ingestion path).
  // ---------------------------------------------------------------------------

  @Test
  public void testExplicitVersionPreserved() throws Exception {
    String capturedVersion = runResolverAndCaptureVersion("0.8.44");
    assertEquals(capturedVersion, "0.8.44");
  }

  @Test
  public void testNullVersionFallsBackToDefault() throws Exception {
    String capturedVersion = runResolverAndCaptureVersion(null);
    assertEquals(capturedVersion, DEFAULT_CLI_VERSION);
  }

  @Test
  public void testEmptyVersionFallsBackToDefault() throws Exception {
    String capturedVersion = runResolverAndCaptureVersion("");
    assertEquals(capturedVersion, DEFAULT_CLI_VERSION);
  }

  @Test
  public void testWhitespaceVersionFallsBackToDefault() throws Exception {
    // Bootstrap YAML templating can render a whitespace-only value; the helper
    // normalizes this to "unset" so we fall through to defaultCliVersion rather
    // than forward a blank version that defeats the fallback at the executor.
    String capturedVersion = runResolverAndCaptureVersion("   ");
    assertEquals(capturedVersion, DEFAULT_CLI_VERSION);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static IngestionConfiguration ingestionConfig() {
    IngestionConfiguration config = new IngestionConfiguration();
    config.setDefaultCliVersion(DEFAULT_CLI_VERSION);
    return config;
  }

  /**
   * Triggers the resolver with the given input.version, captures the persisted
   * MetadataChangeProposal, and returns the {@code args.version} value (or null if absent).
   */
  private static String runResolverAndCaptureVersion(String inputVersion) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfig());

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(new CreateTestConnectionRequestInput("{}", inputVersion));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient).ingestProposal(any(), captor.capture(), anyBoolean());

    String aspectJson = captor.getValue().getAspect().getValue().asString(StandardCharsets.UTF_8);
    JSONObject args = new JSONObject(aspectJson).getJSONObject("args");
    return args.has("version") ? args.getString("version") : null;
  }
}
