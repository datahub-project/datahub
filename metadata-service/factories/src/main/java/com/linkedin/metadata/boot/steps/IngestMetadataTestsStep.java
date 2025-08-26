package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestMode;
import com.linkedin.test.TestStatus;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

/**
 * This bootstrap step is responsible for ingesting default metadata tests
 *
 * <p>For each metadata test defined in the yaml file, it checks whether the urn exists. If not, it
 * ingests the metadata test into DataHub.
 *
 * <p>Note that if a Metadata Tests is soft-deleted by a user, this will NOT re-create the test.
 */
@Slf4j
@RequiredArgsConstructor
public class IngestMetadataTestsStep implements BootstrapStep {

  private final OperationContext systemOperationContext;
  private final EntityService<?> entityService;
  private final TestsConfiguration testsConfiguration;

  private static final String UPGRADE_ID = "ingest-default-metadata-tests-v1";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.BLOCKING;
  }

  @Override
  public String name() {
    return "IngestMetadataTestsStep";
  }

  @Override
  public void execute(@Nonnull OperationContext opContext) throws IOException, URISyntaxException {
    if (entityService.exists(opContext, UPGRADE_ID_URN, true)) {
      log.info("Default metadata tests were already ingested. Skipping ingesting again.");
      return;
    }
    log.info("Ingesting default metadata tests...");

    // If test bootstrap is disabled, skip
    if (!testsConfiguration.isEnabled() || !testsConfiguration.getBootstrap().isEnabled()) {
      log.info("IngestMetadataTestsStep disabled. Skipping.");
      return;
    }

    // 1. Read default metadata tests
    final Map<Urn, TestInfo> metadataTestsMap =
        parseYamlMetadataTestConfig(getYamlResourceStream());

    // 2. Ingest the metadata test if not exists
    log.info("Ingesting {} tests", metadataTestsMap.size());
    int numIngested = 0;
    for (Urn testUrn : metadataTestsMap.keySet()) {
      if (!hasTest(opContext, testUrn)) {
        ingestMetadataTest(opContext, testUrn, metadataTestsMap.get(testUrn));
        numIngested++;
      }
    }

    // 3. Record status
    BootstrapStep.setUpgradeResult(systemOperationContext, UPGRADE_ID_URN, entityService);
    log.info("Ingested {} new tests", numIngested);
  }

  private boolean hasTest(@Nonnull OperationContext opContext, Urn testUrn) {
    // Check if test exists
    try {
      RecordTemplate aspect =
          entityService.getLatestEnvelopedAspect(
              opContext, Constants.TEST_ENTITY_NAME, testUrn, Constants.TEST_INFO_ASPECT_NAME);
      return aspect != null;
    } catch (Exception e) {
      return false;
    }
  }

  private void ingestMetadataTest(
      @Nonnull OperationContext opContext, final Urn testUrn, final TestInfo testInfo) {
    // 3. Write aspect
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(testUrn);
    proposal.setEntityType(Constants.TEST_ENTITY_NAME);
    proposal.setAspectName(Constants.TEST_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(testInfo));
    proposal.setChangeType(ChangeType.CREATE_ENTITY);

    entityService.ingestProposal(
        opContext,
        proposal,
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()),
        false);
  }

  /**
   * Parse yaml metadata test config
   *
   * <p>The structure of yaml must be a list of metadata tests with the necessary fields and test
   * definition.
   */
  private Map<Urn, TestInfo> parseYamlMetadataTestConfig(InputStream inputStream)
      throws IOException {
    // If path does not exist, return empty
    if (inputStream == null) {
      return Collections.emptyMap();
    }

    try {

      final JsonNode metadataTests = systemOperationContext.getYamlMapper().readTree(inputStream);
      if (!metadataTests.isArray()) {
        throw new IllegalArgumentException(
            "Metadata test config file must contain an array of metadata tests");
      }

      Map<Urn, TestInfo> metadataTestsMap = new HashMap<>();

      final long currentTime = System.currentTimeMillis();
      int i = 0;

      for (JsonNode metadataTest : metadataTests) {
        if (!metadataTest.has("urn")) {
          throw new IllegalArgumentException(
              "Each element in the retention config must contain field urn.");
        }
        Urn testUrn = UrnUtils.getUrn(metadataTest.get("urn").asText());
        TestInfo testInfo = new TestInfo();
        if (metadataTest.has("name")) {
          testInfo.setName(metadataTest.get("name").asText());
        } else {
          throw new IllegalArgumentException(
              "Each element in the retention config must contain field name.");
        }

        if (metadataTest.has("category")) {
          testInfo.setCategory(metadataTest.get("category").asText());
        } else {
          throw new IllegalArgumentException(
              "Each element in the retention config must contain field category.");
        }

        if (metadataTest.has("description")) {
          testInfo.setDescription(metadataTest.get("description").asText());
        }

        if (!testsConfiguration.getBootstrap().isActiveDefaults()
            && metadataTest.has("status")
            && metadataTest.get("status").has("mode")) {
          testInfo.setStatus(
              new TestStatus()
                  .setMode(TestMode.valueOf(metadataTest.get("status").get("mode").asText())));
        }

        if (metadataTest.has("definition")) {
          testInfo.setDefinition(
              new TestDefinition()
                  .setType(TestDefinitionType.JSON)
                  .setJson(
                      systemOperationContext
                          .getObjectMapper()
                          .writeValueAsString(metadataTest.get("definition"))));
        } else {
          throw new IllegalArgumentException(
              "Each element in the retention config must contain field definition with the test definition.");
        }

        testInfo.setLastUpdated(
            new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(currentTime + i--));
        metadataTestsMap.put(testUrn, testInfo);
      }
      return metadataTestsMap;
    } catch (Exception e) {
      log.error("Error reading metadata tests file.", e);
      return Collections.emptyMap();
    }
  }

  @VisibleForTesting
  protected InputStream getYamlResourceStream() throws IOException {
    return new ClassPathResource("./boot/metadata_tests.yaml").getInputStream();
  }
}
