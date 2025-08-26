package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.TestsBootstrapConfiguration;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.TestInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the behavior of IngestMetadataTestsStep.
 *
 * <p>We expect it to check if the upgrade has already been performed, and if not, to read metadata
 * tests from a yaml file and ingest them if they don't already exist.
 */
public class IngestMetadataTestsStepTest {
  private static final TestsConfiguration ENABLED_TESTS_CONFIG =
      new TestsConfiguration()
          .setEnabled(true)
          .setBootstrap(new TestsBootstrapConfiguration().setEnabled(true));
  private static final String TEST_URN_1 = "urn:li:test:test-1";
  private static final String TEST_URN_2 = "urn:li:test:test-2";
  private static final Urn UPGRADE_URN =
      BootstrapStep.getUpgradeUrn("ingest-default-metadata-tests-v1");

  private EntityService<?> mockEntityService;
  private OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  private IngestMetadataTestsStep step;

  @BeforeMethod
  public void setUp() {
    mockEntityService = mock(EntityService.class);
  }

  @Test
  public void testExecuteDoesNothingWhenUpgradeAlreadyCompleted() throws Exception {
    // Given: upgrade already completed
    when(mockEntityService.exists(any(OperationContext.class), eq(UPGRADE_URN), eq(true)))
        .thenReturn(true);

    step = new IngestMetadataTestsStep(opContext, mockEntityService, ENABLED_TESTS_CONFIG);

    // When: execute is called
    step.execute(opContext);

    // Then: no metadata tests are ingested
    verify(mockEntityService, times(1))
        .exists(any(OperationContext.class), eq(UPGRADE_URN), eq(true));
    verify(mockEntityService, never()).ingestProposal(any(), any(), any(), anyBoolean());
    verify(mockEntityService, never()).getLatestEnvelopedAspect(any(), any(), any(), any());
  }

  @Test
  public void testExecuteSkipsWhenBootstrapDisabled() throws Exception {
    // Given: bootstrap is disabled
    when(mockEntityService.exists(any(OperationContext.class), eq(UPGRADE_URN), eq(true)))
        .thenReturn(false);

    step =
        new IngestMetadataTestsStep(
            opContext, mockEntityService, ENABLED_TESTS_CONFIG.toBuilder().enabled(false).build());

    // When: execute is called
    step.execute(opContext);

    // Then: no metadata tests are ingested
    verify(mockEntityService, never()).ingestProposal(any(), any(), any(), anyBoolean());
    verify(mockEntityService, never()).getLatestEnvelopedAspect(any(), any(), any(), any());
  }

  @Test
  public void testExecuteIngestsNewMetadataTests() throws Exception {
    // Given: upgrade not completed, bootstrap enabled, and yaml config with 2 tests
    when(mockEntityService.exists(any(OperationContext.class), eq(UPGRADE_URN), eq(true)))
        .thenReturn(false);

    String yamlContent = createValidYamlContent();
    IngestMetadataTestsStep spyStep = createStepWithMocked(yamlContent);

    // First test doesn't exist, second test exists
    when(mockEntityService.getLatestEnvelopedAspect(
            any(OperationContext.class),
            eq(TEST_ENTITY_NAME),
            eq(UrnUtils.getUrn(TEST_URN_1)),
            eq(TEST_INFO_ASPECT_NAME)))
        .thenReturn(null);

    when(mockEntityService.getLatestEnvelopedAspect(
            any(OperationContext.class),
            eq(TEST_ENTITY_NAME),
            eq(UrnUtils.getUrn(TEST_URN_2)),
            eq(TEST_INFO_ASPECT_NAME)))
        .thenReturn(new EnvelopedAspect().setValue(new Aspect(new TestInfo().data()))); // exists

    // When: execute is called
    spyStep.execute(opContext);

    // Then: only the first test is ingested
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService, times(2))
        .ingestProposal(
            any(OperationContext.class),
            proposalCaptor.capture(),
            any(AuditStamp.class),
            anyBoolean());

    // Verify first proposal is for the test
    MetadataChangeProposal testProposal = proposalCaptor.getAllValues().get(0);
    assertEquals(testProposal.getEntityUrn(), UrnUtils.getUrn(TEST_URN_1));
    assertEquals(testProposal.getEntityType(), TEST_ENTITY_NAME);
    assertEquals(testProposal.getAspectName(), TEST_INFO_ASPECT_NAME);

    // Verify second proposal is for the upgrade status
    MetadataChangeProposal upgradeProposal = proposalCaptor.getAllValues().get(1);
    assertEquals(upgradeProposal.getEntityUrn(), UPGRADE_URN);
  }

  @Test
  public void testExecuteHandlesInvalidYamlGracefully() throws Exception {
    // Given: invalid yaml content
    when(mockEntityService.exists(any(OperationContext.class), eq(UPGRADE_URN), eq(true)))
        .thenReturn(false);

    String invalidYaml = "not an array";
    IngestMetadataTestsStep spyStep = createStepWithMocked(invalidYaml);

    // When: execute is called
    spyStep.execute(opContext);

    // Then: no tests are ingested, but upgrade is marked as complete
    verify(mockEntityService, times(1))
        .ingestProposal(
            any(OperationContext.class),
            argThat(proposal -> proposal.getEntityUrn().equals(UPGRADE_URN)),
            any(AuditStamp.class),
            eq(false));
  }

  @Test
  public void testExecuteValidatesRequiredYamlFields() throws Exception {
    // Given: yaml with missing required fields
    when(mockEntityService.exists(any(OperationContext.class), eq(UPGRADE_URN), eq(true)))
        .thenReturn(false);

    String yamlMissingName =
        "[{\"urn\": \""
            + TEST_URN_1
            + "\", \"category\": \"Test Category\", \"definition\": {\"type\": \"json\"}}]";
    IngestMetadataTestsStep spyStep = createStepWithMocked(yamlMissingName);

    // When: execute is called
    spyStep.execute(opContext);

    // Then: no tests are ingested due to validation failure
    verify(mockEntityService, never())
        .ingestProposal(
            any(),
            argThat(proposal -> TEST_URN_1.equals(proposal.getEntityUrn().toString())),
            any(),
            anyBoolean());

    // But upgrade is still marked as complete
    verify(mockEntityService, times(1))
        .ingestProposal(
            any(OperationContext.class),
            argThat(proposal -> proposal.getEntityUrn().equals(UPGRADE_URN)),
            any(AuditStamp.class),
            eq(false));
  }

  @Test
  public void testExecuteHandlesOptionalFields() throws Exception {
    // Given: yaml with only required fields
    when(mockEntityService.exists(any(OperationContext.class), eq(UPGRADE_URN), eq(true)))
        .thenReturn(false);

    String minimalYaml =
        "[{"
            + "\"urn\": \""
            + TEST_URN_1
            + "\", "
            + "\"name\": \"Test 1\", "
            + "\"category\": \"Test Category\", "
            + "\"definition\": {\"type\": \"json\", \"config\": {}}"
            + "}]";

    IngestMetadataTestsStep spyStep = createStepWithMocked(minimalYaml);

    when(mockEntityService.getLatestEnvelopedAspect(any(), any(), any(), any())).thenReturn(null);

    // When: execute is called
    spyStep.execute(opContext);

    // Then: test is ingested successfully without optional fields
    verify(mockEntityService, times(1))
        .ingestProposal(
            any(OperationContext.class),
            argThat(proposal -> TEST_URN_1.equals(proposal.getEntityUrn().toString())),
            any(AuditStamp.class),
            eq(false));
  }

  @Test
  public void testGetExecutionMode() {
    step = new IngestMetadataTestsStep(opContext, mockEntityService, ENABLED_TESTS_CONFIG);
    assertEquals(step.getExecutionMode(), BootstrapStep.ExecutionMode.BLOCKING);
  }

  @Test
  public void testGetName() {
    step = new IngestMetadataTestsStep(opContext, mockEntityService, ENABLED_TESTS_CONFIG);
    assertEquals(step.name(), "IngestMetadataTestsStep");
  }

  @Test
  public void testHandleExceptionDuringTestExistenceCheck() throws Exception {
    // Given: exception thrown when checking if test exists
    when(mockEntityService.exists(any(OperationContext.class), eq(UPGRADE_URN), eq(true)))
        .thenReturn(false);

    String yamlContent = createValidYamlContent();
    IngestMetadataTestsStep spyStep = createStepWithMocked(yamlContent);

    when(mockEntityService.getLatestEnvelopedAspect(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Database error"));

    // When: execute is called
    spyStep.execute(opContext);

    // Then: test is treated as non-existent and ingested and result status updated
    verify(mockEntityService, times(3))
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            anyBoolean());
  }

  private String createValidYamlContent() {
    return "["
        + "{"
        + "  \"urn\": \""
        + TEST_URN_1
        + "\","
        + "  \"name\": \"Test 1\","
        + "  \"category\": \"Freshness\","
        + "  \"description\": \"Test description 1\","
        + "  \"status\": {\"mode\": \"ACTIVE\"},"
        + "  \"definition\": {\"type\": \"freshness\", \"config\": {\"maxAge\": 86400}}"
        + "},"
        + "{"
        + "  \"urn\": \""
        + TEST_URN_2
        + "\","
        + "  \"name\": \"Test 2\","
        + "  \"category\": \"Volume\","
        + "  \"description\": \"Test description 2\","
        + "  \"status\": {\"mode\": \"INACTIVE\"},"
        + "  \"definition\": {\"type\": \"volume\", \"config\": {\"minRows\": 1000}}"
        + "}"
        + "]";
  }

  private IngestMetadataTestsStep createStepWithMocked(String yamlContent) throws Exception {
    IngestMetadataTestsStep spyStep =
        spy(new IngestMetadataTestsStep(opContext, mockEntityService, ENABLED_TESTS_CONFIG));
    InputStream yamlStream = new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8));
    doReturn(yamlStream).when(spyStep).getYamlResourceStream();
    return spyStep;
  }
}
