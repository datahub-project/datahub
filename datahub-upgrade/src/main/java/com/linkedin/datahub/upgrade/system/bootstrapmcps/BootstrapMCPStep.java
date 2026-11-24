package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.model.BootstrapMCPConfigFile;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This bootstrap step is responsible for upgrading DataHub policy documents with new searchable
 * fields in ES
 */
@Slf4j
public class BootstrapMCPStep implements UpgradeStep {
  static final String VALUES_HASH_KEY = "valuesHash";

  private final String upgradeId;
  private final Urn upgradeIdUrn;

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  @Getter private final BootstrapMCPConfigFile.MCPTemplate mcpTemplate;

  public BootstrapMCPStep(
      OperationContext opContext,
      EntityService<?> entityService,
      BootstrapMCPConfigFile.MCPTemplate mcpTemplate) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.mcpTemplate = mcpTemplate;
    this.upgradeId =
        String.join("-", List.of("bootstrap", mcpTemplate.getName(), mcpTemplate.getVersion()));
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(this.upgradeId);
  }

  @Override
  public String id() {
    return upgradeId;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        AspectsBatch batch = BootstrapMCPUtil.generateAspectBatch(opContext, mcpTemplate, id());
        log.info("Ingesting {} MCPs", batch.getItems().size());
        entityService.ingestProposal(opContext, batch, mcpTemplate.isAsync());
      } catch (IOException e) {
        log.error("Error bootstrapping MCPs", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      if (mcpTemplate.isValuesTrackedAcrossRuns()) {
        try {
          BootstrapStep.setUpgradeResult(
              context.opContext(),
              upgradeIdUrn,
              entityService,
              Map.of(VALUES_HASH_KEY, BootstrapMCPUtil.valuesHash(opContext, mcpTemplate)));
        } catch (IOException e) {
          log.error("Error hashing bootstrap MCP values", e);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
      } else {
        BootstrapStep.setUpgradeResult(context.opContext(), upgradeIdUrn, entityService);
      }

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return mcpTemplate.isOptional();
  }

  /** Returns whether the upgrade should be skipped. */
  @Override
  public boolean skip(UpgradeContext context) {
    if (!mcpTemplate.isForce() && mcpTemplate.isValuesTrackedAcrossRuns()) {
      return valuesUnchangedSinceLastRun(context);
    }
    if (!mcpTemplate.isForce()) {
      boolean previouslyRun =
          entityService.exists(
              context.opContext(), upgradeIdUrn, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
      if (previouslyRun) {
        log.info("{} was already run. Skipping.", id());
      }
      return previouslyRun;
    } else {
      log.info("{} forced run.", id());
      return false;
    }
  }

  private boolean valuesUnchangedSinceLastRun(UpgradeContext context) {
    Optional<DataHubUpgradeResult> stored =
        BootstrapStep.getUpgradeResult(context.opContext(), upgradeIdUrn, entityService);
    String storedHash =
        stored
            .filter(DataHubUpgradeResult::hasResult)
            .map(result -> result.getResult().get(VALUES_HASH_KEY))
            .orElse(null);
    if (storedHash == null) {
      log.info("{} has no stored values hash. Running.", id());
      return false;
    }
    try {
      boolean unchanged = storedHash.equals(BootstrapMCPUtil.valuesHash(opContext, mcpTemplate));
      log.info(
          unchanged ? "{} values are unchanged. Skipping." : "{} values changed. Running.", id());
      return unchanged;
    } catch (IOException e) {
      // The run renders the same values and reports the parse failure as its own error.
      log.warn("Could not hash values for {}. Running.", id(), e);
      return false;
    }
  }
}
