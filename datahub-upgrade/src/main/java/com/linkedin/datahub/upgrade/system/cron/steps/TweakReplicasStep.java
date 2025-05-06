package com.linkedin.datahub.upgrade.system.cron.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.cron.CronArgs;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TweakReplicasStep implements UpgradeStep {

  private final List<ElasticSearchIndexed> services;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  @Getter private CronArgs args;

  @Override
  public String id() {
    return TweakReplicasStep.class.getSimpleName();
  }

  private boolean getDryRun(final Map<String, Optional<String>> parsedArgs) {
    Boolean dryRun = null;
    if (containsKey(parsedArgs, "dryRun")) {
      dryRun = Boolean.parseBoolean(parsedArgs.get("dryRun").get());
    }
    return dryRun != null ? dryRun : false;
  }

  public CronArgs createArgs(UpgradeContext context) {
    if (args != null) {
      return args;
    } else {
      CronArgs result = new CronArgs();
      result.dryRun = getDryRun(context.parsedArgs());
      args = result;
      return result;
    }
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      CronArgs args = createArgs(context);
      try {
        for (ElasticSearchIndexed service : services) {
          service.tweakReplicasAll(structuredProperties, args.dryRun);
        }
      } catch (Exception e) {
        log.error("TweakReplicasStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
