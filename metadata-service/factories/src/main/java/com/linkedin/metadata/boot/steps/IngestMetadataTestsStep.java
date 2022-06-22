package com.linkedin.metadata.boot.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import java.io.File;
import java.io.IOException;
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
 * For each metadata test defined in the yaml file, it checks whether the urn exists.
 * If not, it ingests the metadata test into DataHub.
 */
@Slf4j
@RequiredArgsConstructor
public class IngestMetadataTestsStep implements BootstrapStep {

  private final EntityService _entityService;
  private final boolean _enableMetadataTestBootstrap;

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

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
  public void execute() throws IOException, URISyntaxException {
    log.info("Ingesting default metadata tests...");

    // If test bootstrap is disabled, skip
    if (!_enableMetadataTestBootstrap) {
      log.info("IngestMetadataTestsStep disabled. Skipping.");
      return;
    }

    // 1. Read default retention config
    final Map<Urn, TestInfo> metadataTestsMap =
        parseYamlMetadataTestConfig(new ClassPathResource("./boot/metadata_tests.yaml").getFile());

    // 2. Ingest the metadata test if not exists
    log.info("Ingesting {} tests", metadataTestsMap.size());
    int numIngested = 0;
    for (Urn testUrn : metadataTestsMap.keySet()) {
      if (!hasTest(testUrn)) {
        ingestMetadataTest(testUrn, metadataTestsMap.get(testUrn));
        numIngested++;
      }
    }
    log.info("Ingested {} new tests", numIngested);
  }

  private boolean hasTest(Urn testUrn) {
    // Check if policy exists
    try {
      RecordTemplate aspect =
          _entityService.getLatestEnvelopedAspect(Constants.TEST_ENTITY_NAME, testUrn, Constants.TEST_INFO_ASPECT_NAME);
      return aspect != null;
    } catch (Exception e) {
      return false;
    }
  }

  private void ingestMetadataTest(final Urn testUrn, final TestInfo testInfo) {
    // 3. Write key & aspect
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(testUrn);
    GenericAspect aspect = GenericRecordUtils.serializeAspect(
        EntityKeyUtils.convertUrnToEntityKey(testUrn, keyAspectSpec.getPegasusSchema()));
    keyAspectProposal.setAspect(aspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(Constants.TEST_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(testUrn);

    _entityService.ingestProposal(keyAspectProposal,
        new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(testUrn);
    proposal.setEntityType(Constants.TEST_ENTITY_NAME);
    proposal.setAspectName(Constants.TEST_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(testInfo));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(proposal,
        new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));
  }

  /**
   * Parse yaml metadata test config
   *
   * The structure of yaml must be a list of metadata tests with the necessary fields and test definition.
   */
  private Map<Urn, TestInfo> parseYamlMetadataTestConfig(File metadataTestsFile) throws IOException {
    // If path does not exist, return empty
    if (!metadataTestsFile.exists()) {
      return Collections.emptyMap();
    }

    // If file is not a yaml file, return empty
    if (!metadataTestsFile.getPath().endsWith(".yaml") && metadataTestsFile.getPath().endsWith(".yml")) {
      log.info("File {} is not a YAML file. Skipping", metadataTestsFile.getPath());
      return Collections.emptyMap();
    }

    final JsonNode metadataTests = YAML_MAPPER.readTree(metadataTestsFile);
    if (!metadataTests.isArray()) {
      throw new IllegalArgumentException("Metadata test config file must contain an array of metadata tests");
    }

    Map<Urn, TestInfo> metadataTestsMap = new HashMap<>();

    for (JsonNode metadataTest : metadataTests) {
      if (!metadataTest.has("urn")) {
        throw new IllegalArgumentException("Each element in the retention config must contain field urn.");
      }
      Urn testUrn = UrnUtils.getUrn(metadataTest.get("urn").asText());
      TestInfo testInfo = new TestInfo();
      if (metadataTest.has("name")) {
        testInfo.setName(metadataTest.get("name").asText());
      } else {
        throw new IllegalArgumentException("Each element in the retention config must contain field name.");
      }

      if (metadataTest.has("category")) {
        testInfo.setCategory(metadataTest.get("category").asText());
      } else {
        throw new IllegalArgumentException("Each element in the retention config must contain field category.");
      }

      if (metadataTest.has("description")) {
        testInfo.setDescription(metadataTest.get("description").asText());
      }

      if (metadataTest.has("definition")) {
        testInfo.setDefinition(new TestDefinition().setType(TestDefinitionType.JSON)
            .setJson(JSON_MAPPER.writeValueAsString(metadataTest.get("definition"))));
      } else {
        throw new IllegalArgumentException(
            "Each element in the retention config must contain field definition with the test definition.");
      }

      metadataTestsMap.put(testUrn, testInfo);
    }
    return metadataTestsMap;
  }
}
