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
import javax.annotation.Nullable;
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
  // Keyed by template name only, so it does not change with the version or a revision override.
  private final Urn lastAppliedValuesUrn;

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
    this.lastAppliedValuesUrn =
        BootstrapStep.getUpgradeUrn(
            String.join("-", List.of("bootstrap", mcpTemplate.getName(), "last-applied-values")));
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
          Map<String, String> valuesHash =
              Map.of(VALUES_HASH_KEY, BootstrapMCPUtil.valuesHash(opContext, mcpTemplate));
          // The last applied record goes first. If the id's own result then fails to write, that
          // id keeps an older hash and cannot be skipped, so a later return to it still applies.
          // The other order would leave the last applied record on the older values.
          BootstrapStep.setUpgradeResult(
              context.opContext(), lastAppliedValuesUrn, entityService, valuesHash);
          BootstrapStep.setUpgradeResult(
              context.opContext(), upgradeIdUrn, entityService, valuesHash);
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

  /**
   * The values are unchanged only when this upgrade id's own result and the template's last applied
   * values both match the current hash. The id can carry the values themselves, as the Helm chart
   * puts a hash of them in the revision override, so turning a value off and back on at one version
   * returns to an id whose result still matches. The last applied record is what shows that the
   * schedule written since then was for different values.
   */
  private boolean valuesUnchangedSinceLastRun(UpgradeContext context) {
    String storedHash = storedValuesHash(context, upgradeIdUrn);
    if (storedHash == null) {
      log.info("{} has no stored values hash. Running.", id());
      return false;
    }
    String lastAppliedHash = storedValuesHash(context, lastAppliedValuesUrn);
    if (lastAppliedHash == null) {
      log.info("{} has no record of the last applied values. Running.", id());
      return false;
    }
    try {
      String currentHash = BootstrapMCPUtil.valuesHash(opContext, mcpTemplate);
      boolean unchanged = storedHash.equals(currentHash) && lastAppliedHash.equals(currentHash);
      log.info(
          unchanged ? "{} values are unchanged. Skipping." : "{} values changed. Running.", id());
      return unchanged;
    } catch (IOException e) {
      // The run renders the same values and reports the parse failure as its own error.
      log.warn("Could not hash values for {}. Running.", id(), e);
      return false;
    }
  }

  @Nullable
  private String storedValuesHash(UpgradeContext context, Urn urn) {
    Optional<DataHubUpgradeResult> stored =
        BootstrapStep.getUpgradeResult(context.opContext(), urn, entityService);
    return stored
        .filter(DataHubUpgradeResult::hasResult)
        .map(result -> result.getResult().get(VALUES_HASH_KEY))
        .orElse(null);
  }
}
