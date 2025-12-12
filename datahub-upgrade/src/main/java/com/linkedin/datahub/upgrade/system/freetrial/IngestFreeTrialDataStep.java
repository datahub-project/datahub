package com.linkedin.datahub.upgrade.system.freetrial;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.McpJsonUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;

/**
 * Upgrade step to ingest sample data for free trial instances.
 *
 * <p>This step only executes if IS_FREE_TRIAL_INSTANCE env var is set to true.
 *
 * <p>The JSON file format is a JSON array of standard MCPs.
 */
@Slf4j
public class IngestFreeTrialDataStep implements UpgradeStep {

  private static final String UPGRADE_ID_PREFIX = "IngestFreeTrialData";
  private static final String DEFAULT_FILE_PATH = "boot/sample_data_mcp.json";
  private static final int MAX_PARSE_ERRORS_TO_LOG = 10;
  private static final String SAMPLE_DATA_PROPERTY_KEY = "sampleData";
  private static final String SAMPLE_DATA_PROPERTY_VALUE = "true";

  private final OperationContext systemOpContext;
  private final EntityService<?> entityService;
  private final boolean enabled;
  private final boolean reprocessEnabled;
  private final int batchSize;
  private final String filePath;
  private final String upgradeId;
  private final Urn upgradeIdUrn;

  public IngestFreeTrialDataStep(
      OperationContext systemOpContext,
      EntityService<?> entityService,
      boolean enabled,
      boolean reprocessEnabled,
      int batchSize) {
    this(systemOpContext, entityService, enabled, reprocessEnabled, batchSize, DEFAULT_FILE_PATH);
  }

  public IngestFreeTrialDataStep(
      OperationContext systemOpContext,
      EntityService<?> entityService,
      boolean enabled,
      boolean reprocessEnabled,
      int batchSize,
      String filePath) {
    this.systemOpContext = systemOpContext;
    this.entityService = entityService;
    this.enabled = enabled;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
    this.filePath = filePath;
    this.upgradeId = UPGRADE_ID_PREFIX + "-" + computeFileHash(filePath);
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(this.upgradeId);
  }

  @Override
  public String id() {
    return upgradeId;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final ObjectMapper mapper = context.opContext().getObjectMapper();

      try {
        log.info("Starting ingestion of free trial sample data from: {}", filePath);

        final List<MetadataChangeProposal> allProposals;
        try (InputStream inputStream = new ClassPathResource(filePath).getInputStream()) {
          allProposals =
              McpJsonUtils.parseMcpsFromJsonStream(mapper, inputStream, MAX_PARSE_ERRORS_TO_LOG);
        } catch (Exception e) {
          log.error(
              "Could not parse sample data file at {}. Error: {}", filePath, e.getMessage(), e);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        log.info("Parsed {} MCPs. Marking as sample data...", allProposals.size());

        // Mark all MCPs with sampleData=true in systemMetadata for easy identification
        for (MetadataChangeProposal mcp : allProposals) {
          markAsSampleData(mcp);
        }

        log.info("Starting batch ingestion...");

        // Batch and ingest
        final AuditStamp auditStamp =
            new AuditStamp()
                .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis());

        final AtomicInteger batchesProcessed = new AtomicInteger(0);
        final AtomicInteger batchesFailed = new AtomicInteger(0);

        Iterators.partition(allProposals.iterator(), batchSize)
            .forEachRemaining(
                batch -> {
                  try {
                    final AspectsBatch aspectsBatch =
                        AspectsBatchImpl.builder()
                            .mcps(batch, auditStamp, context.opContext().getRetrieverContext())
                            .build(context.opContext());

                    entityService.ingestProposal(context.opContext(), aspectsBatch, true);

                    batchesProcessed.incrementAndGet();
                    log.info(
                        "Ingested batch {}. Progress: {} / {}",
                        batchesProcessed.get(),
                        Math.min(batchesProcessed.get() * batchSize, allProposals.size()),
                        allProposals.size());
                  } catch (Exception e) {
                    batchesFailed.incrementAndGet();
                    log.error(
                        "Failed to ingest batch {}: {}",
                        batchesProcessed.get() + 1,
                        e.getMessage(),
                        e);
                  }
                });

        // Only mark upgrade as complete if all batches succeeded
        if (batchesFailed.get() > 0) {
          log.error(
              "Failed to ingest free trial sample data: {} of {} batches failed.",
              batchesFailed.get(),
              batchesProcessed.get() + batchesFailed.get());
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        BootstrapStep.setUpgradeResult(context.opContext(), upgradeIdUrn, entityService);

        log.info(
            "Completed ingestion of free trial sample data. MCPs: {}, Batches: {} success",
            allProposals.size(),
            batchesProcessed.get());

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error(
            "Failed to ingest free trial sample data from {}: {}", filePath, e.getMessage(), e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  @Override
  public boolean isOptional() {
    return false;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (!enabled) {
      log.debug("IngestFreeTrialData is disabled. Skipping.");
      return true;
    }

    if (reprocessEnabled) {
      log.info("IngestFreeTrialData reprocess enabled. Running.");
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            systemOpContext, upgradeIdUrn, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }

  /**
   * Computes a short hash of the file content for versioning. Returns first 8 chars of SHA-256
   * hash.
   *
   * @throws IllegalStateException if the file cannot be read or hash cannot be computed
   */
  private static String computeFileHash(String filePath) {
    try (InputStream inputStream = new ClassPathResource(filePath).getInputStream()) {
      String content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
      StringBuilder hexString = new StringBuilder();
      for (int i = 0; i < 4; i++) { // First 4 bytes = 8 hex chars
        hexString.append(String.format("%02x", hash[i]));
      }
      return hexString.toString();
    } catch (IOException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Could not compute hash for " + filePath, e);
    }
  }

  /**
   * Marks an MCP with sampleData=true in systemMetadata.properties. This allows easy identification
   * and bulk operations on sample data.
   */
  private void markAsSampleData(MetadataChangeProposal mcp) {
    SystemMetadata systemMetadata = mcp.getSystemMetadata();
    if (systemMetadata == null) {
      systemMetadata = new SystemMetadata();
    }

    StringMap properties = systemMetadata.getProperties();
    if (properties == null) {
      properties = new StringMap();
    }

    properties.put(SAMPLE_DATA_PROPERTY_KEY, SAMPLE_DATA_PROPERTY_VALUE);
    systemMetadata.setProperties(properties);
    mcp.setSystemMetadata(systemMetadata);
  }
}
