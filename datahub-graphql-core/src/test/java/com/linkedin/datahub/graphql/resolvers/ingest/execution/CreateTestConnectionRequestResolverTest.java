package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.*;

import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateTestConnectionRequestInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateTestConnectionRequestResolverTest {

  private static final String DEFAULT_VERSION = "0.14.0";
  private static final String EXPLICIT_VERSION = "0.8.44";
  private static final String MATRIX_SNOWFLAKE_VERSION = "0.13.0.1";
  private static final String SNOWFLAKE_RECIPE =
      "{\"source\":{\"type\":\"snowflake\",\"config\":{\"account_id\":\"abc123\"}}}";
  private static final String RECIPE_WITHOUT_TYPE =
      "{\"source\":{\"config\":{\"account_id\":\"abc123\"}}}";

  private static final CreateTestConnectionRequestInput TEST_INPUT_WITH_VERSION =
      new CreateTestConnectionRequestInput(SNOWFLAKE_RECIPE, EXPLICIT_VERSION);
  private static final CreateTestConnectionRequestInput TEST_INPUT_NO_VERSION =
      new CreateTestConnectionRequestInput(SNOWFLAKE_RECIPE, null);

  /**
   * A matrix service backed by a {@link NoOpIngestionCliVersionMatrixSource} — what production
   * wires when no matrix backend is configured. Always returns an empty matrix (so resolution falls
   * through to {@code defaultCliVersion}) and reports no server version.
   */
  private static IngestionCliVersionMatrixService disabledMatrixService() {
    return new IngestionCliVersionMatrixService(
        new NoOpIngestionCliVersionMatrixSource(), null, null);
  }

  @Test
  public void testExplicitInputVersionWins() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);

    IngestionCliVersionMatrixService matrix = Mockito.mock(IngestionCliVersionMatrixService.class);
    Mockito.when(matrix.resolveVersionWithSource("snowflake"))
        .thenReturn(
            Optional.of(
                new IngestionCliVersionMatrixService.MatrixResolution(
                    MATRIX_SNOWFLAKE_VERSION,
                    IngestionCliVersionMatrixService.MatrixSourceLevel.COHORT)));

    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfiguration, matrix);

    runAndVerifyVersion(resolver, mockClient, TEST_INPUT_WITH_VERSION, EXPLICIT_VERSION);

    // Even though the matrix has a value for snowflake, the explicit input.version should win.
    // The resolver may short-circuit before consulting the matrix at all — that's an optimization,
    // not a contract, so we only assert the resolved version here.
  }

  @Test
  public void testMatrixConnectorVersionUsedWhenInputVersionMissing() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);

    IngestionCliVersionMatrixService matrix = Mockito.mock(IngestionCliVersionMatrixService.class);
    Mockito.when(matrix.resolveVersionWithSource("snowflake"))
        .thenReturn(
            Optional.of(
                new IngestionCliVersionMatrixService.MatrixResolution(
                    MATRIX_SNOWFLAKE_VERSION,
                    IngestionCliVersionMatrixService.MatrixSourceLevel.COHORT)));

    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfiguration, matrix);

    runAndVerifyVersion(resolver, mockClient, TEST_INPUT_NO_VERSION, MATRIX_SNOWFLAKE_VERSION);
  }

  @Test
  public void testFallsBackToDefaultCliVersionWhenNoVersionAndNoMatrix() throws Exception {
    // Closes the prior gap where test connections silently omitted version when input.version was
    // null, instead of falling back to defaultCliVersion like real executions do.
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);

    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(
            mockClient, ingestionConfiguration, disabledMatrixService());

    runAndVerifyVersion(resolver, mockClient, TEST_INPUT_NO_VERSION, DEFAULT_VERSION);
  }

  @Test
  public void testEmptyVersionFallsBackToDefault() throws Exception {
    // Bootstrap YAML templating can render input.version as an empty string (or whitespace-only)
    // when the source has no version pin; the helper normalizes that to "unset" so we still fall
    // through to defaultCliVersion. Without this, the blank would forward to the executor and
    // silently pin to its bundled CLI.
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);

    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(
            mockClient, ingestionConfiguration, disabledMatrixService());

    runAndVerifyVersion(
        resolver,
        mockClient,
        new CreateTestConnectionRequestInput(SNOWFLAKE_RECIPE, ""),
        DEFAULT_VERSION);
  }

  @Test
  public void testWhitespaceVersionFallsBackToDefault() throws Exception {
    // Same as the empty-string case but for whitespace-only — also normalized to "unset".
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);

    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(
            mockClient, ingestionConfiguration, disabledMatrixService());

    runAndVerifyVersion(
        resolver,
        mockClient,
        new CreateTestConnectionRequestInput(SNOWFLAKE_RECIPE, "   "),
        DEFAULT_VERSION);
  }

  @Test
  public void testFallsBackToDefaultWhenMatrixHasNoEntryForConnector() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);

    IngestionCliVersionMatrixService matrix = Mockito.mock(IngestionCliVersionMatrixService.class);
    Mockito.when(matrix.resolveVersionWithSource("snowflake")).thenReturn(Optional.empty());

    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfiguration, matrix);

    runAndVerifyVersion(resolver, mockClient, TEST_INPUT_NO_VERSION, DEFAULT_VERSION);
  }

  @Test
  public void testFallsBackToDefaultWhenRecipeHasNoSourceType() throws Exception {
    // Valid JSON, but source.type is missing — we cannot identify the connector for a matrix
    // lookup, so we must fall through to defaultCliVersion rather than crash or pick a wrong pin.
    // (Truly malformed JSON is rejected earlier by IngestionUtils.injectPipelineName, so we don't
    // exercise that path here.)
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);

    IngestionCliVersionMatrixService matrix = Mockito.mock(IngestionCliVersionMatrixService.class);
    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfiguration, matrix);

    CreateTestConnectionRequestInput input =
        new CreateTestConnectionRequestInput(RECIPE_WITHOUT_TYPE, null);
    runAndVerifyVersion(resolver, mockClient, input, DEFAULT_VERSION);

    // We never attempt a matrix lookup because we cannot identify the connector type.
    Mockito.verify(matrix, Mockito.never()).resolveVersionWithSource(Mockito.anyString());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);
    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(
            mockClient, ingestionConfiguration, disabledMatrixService());

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_WITH_VERSION);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  /**
   * Forensic stamp: the resolution record on the ExecutionRequestInput must reflect which
   * resolution path actually fired (cohort vs connector default vs workspace default), with
   * matching version + cohort index + matrix server-version metadata. This is the structured audit
   * trail downstream tooling queries — not just args.version.
   */
  @Test
  public void testStampsResolutionMetadata_cohortMatch() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);

    IngestionCliVersionMatrixService matrix = Mockito.mock(IngestionCliVersionMatrixService.class);
    Mockito.when(matrix.resolveVersionWithSource("snowflake"))
        .thenReturn(
            Optional.of(
                new IngestionCliVersionMatrixService.MatrixResolution(
                    MATRIX_SNOWFLAKE_VERSION,
                    IngestionCliVersionMatrixService.MatrixSourceLevel.COHORT)));
    Mockito.when(matrix.getServerVersion()).thenReturn("1.3.1.4");

    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfiguration, matrix);

    ExecutionRequestInput captured =
        runAndCaptureResolution(resolver, mockClient, TEST_INPUT_NO_VERSION);

    assertEquals(captured.getArgs().get("version"), MATRIX_SNOWFLAKE_VERSION);
    com.linkedin.execution.CliVersionAudit stamp = captured.getCliVersionAudit();
    assertEquals(stamp.getSource(), com.linkedin.execution.CliVersionSource.MATRIX_COHORT);
    assertEquals(stamp.getServerVersion(), "1.3.1.4");
  }

  @Test
  public void testStampsResolutionMetadata_perSourceOverride() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);
    // Matrix backend disabled (NoOp source, no server version) — the explicit version must still
    // produce a SOURCE_CONFIG_OVERRIDE stamp.
    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(
            mockClient, ingestionConfiguration, disabledMatrixService());

    ExecutionRequestInput captured =
        runAndCaptureResolution(resolver, mockClient, TEST_INPUT_WITH_VERSION);

    assertEquals(captured.getArgs().get("version"), EXPLICIT_VERSION);
    com.linkedin.execution.CliVersionAudit stamp = captured.getCliVersionAudit();
    assertEquals(stamp.getSource(), com.linkedin.execution.CliVersionSource.SOURCE_CONFIG_OVERRIDE);
    // The disabled matrix service reports no server version, so none is stamped.
    assertFalse(stamp.hasServerVersion());
  }

  @Test
  public void testStampsResolutionMetadata_applicationDefault() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion(DEFAULT_VERSION);

    IngestionCliVersionMatrixService matrix = Mockito.mock(IngestionCliVersionMatrixService.class);
    Mockito.when(matrix.resolveVersionWithSource("snowflake")).thenReturn(Optional.empty());
    Mockito.when(matrix.getServerVersion()).thenReturn("1.3.1.4");

    CreateTestConnectionRequestResolver resolver =
        new CreateTestConnectionRequestResolver(mockClient, ingestionConfiguration, matrix);

    ExecutionRequestInput captured =
        runAndCaptureResolution(resolver, mockClient, TEST_INPUT_NO_VERSION);

    assertEquals(captured.getArgs().get("version"), DEFAULT_VERSION);
    com.linkedin.execution.CliVersionAudit stamp = captured.getCliVersionAudit();
    assertEquals(stamp.getSource(), com.linkedin.execution.CliVersionSource.APPLICATION_DEFAULT);
    // serverVersion is stamped even on APPLICATION_DEFAULT when the matrix service is wired.
    assertEquals(stamp.getServerVersion(), "1.3.1.4");
  }

  /**
   * Captures the proposal and returns the full {@link ExecutionRequestInput}, so tests can assert
   * on both {@code args.version} (where the CLI version string lives) and {@code cliVersionAudit}
   * (where the audit stamp lives).
   */
  private static ExecutionRequestInput runAndCaptureResolution(
      CreateTestConnectionRequestResolver resolver,
      EntityClient mockClient,
      CreateTestConnectionRequestInput input)
      throws Exception {
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient).ingestProposal(any(), proposalCaptor.capture(), Mockito.eq(false));
    MetadataChangeProposal proposal = proposalCaptor.getValue();

    ExecutionRequestInput recovered =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            ExecutionRequestInput.class);
    assertTrue(
        recovered.hasCliVersionAudit(),
        "Expected cliVersionAudit to be stamped on the ExecutionRequestInput");
    return recovered;
  }

  /**
   * Executes the resolver against an allow-context and asserts that the version argument on the
   * resulting {@link ExecutionRequestInput} matches {@code expectedVersion}. Captures the {@link
   * MetadataChangeProposal} written to the entity client and rehydrates the {@code
   * ExecutionRequestInput} aspect to inspect the resolved args.
   */
  private static void runAndVerifyVersion(
      CreateTestConnectionRequestResolver resolver,
      EntityClient mockClient,
      CreateTestConnectionRequestInput input,
      String expectedVersion)
      throws Exception {
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient).ingestProposal(any(), proposalCaptor.capture(), Mockito.eq(false));

    MetadataChangeProposal proposal = proposalCaptor.getValue();
    assertEquals(proposal.getEntityType(), Constants.EXECUTION_REQUEST_ENTITY_NAME);
    assertEquals(proposal.getAspectName(), Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);

    ExecutionRequestInput recovered =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            ExecutionRequestInput.class);
    StringMap args = recovered.getArgs();
    assertNotNull(args, "Expected args to be populated on the ExecutionRequestInput");
    assertEquals(args.get("version"), expectedVersion);
  }
}
