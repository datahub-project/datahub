package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.AbstractPersistentUpgradeStep;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.model.BootstrapMCPConfigFile;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This bootstrap step is responsible for upgrading DataHub policy documents with new searchable
 * fields in ES
 */
@Slf4j
public class BootstrapMCPStep extends AbstractPersistentUpgradeStep {
  private final String upgradeId;

  @Getter private final BootstrapMCPConfigFile.MCPTemplate mcpTemplate;

  public BootstrapMCPStep(
      OperationContext opContext,
      EntityService<?> entityService,
      BootstrapMCPConfigFile.MCPTemplate mcpTemplate) {
    super(opContext, entityService);
    this.mcpTemplate = mcpTemplate;
    this.upgradeId =
        String.join("-", List.of("bootstrap", mcpTemplate.getName(), mcpTemplate.getVersion()));
  }

  @Override
  public String id() {
    return upgradeId;
  }

  @Override
  public boolean isReprocessEnabled() {
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        AspectsBatch batch =
            BootstrapMCPUtil.generateAspectBatch(getSystemOpContext(), mcpTemplate, id());
        log.info("Ingesting {} MCPs", batch.getItems().size());
        getEntityService().ingestProposal(getSystemOpContext(), batch, mcpTemplate.isAsync());
      } catch (IOException e) {
        log.error("Error bootstrapping MCPs", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
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
    if (mcpTemplate.isForce()) {
      log.info("{} forced run.", id());
      return false; // Force run even if previously completed
    }
    return PersistentUpgradeStep.super.skip(context); // Use interface default
  }
}
