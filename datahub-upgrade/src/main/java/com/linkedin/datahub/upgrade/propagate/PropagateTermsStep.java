package com.linkedin.datahub.upgrade.propagate;

import com.datahub.util.RecordUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import java.util.Optional;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class PropagateTermsStep implements UpgradeStep {

  private final EntityService _entityService;
  private final EntitySearchService _entitySearchService;
  private final EntityRegistry _entityRegistry;

  @Override
  public String id() {
    return "PropagateTermsStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      context.report().addLine("Starting term propagation...");

      Optional<String> sourceFilter = context.parsedArgs().get("SOURCE_FILTER");
      if (!sourceFilter.isPresent()) {
        context.report().addLine("Missing required arguments. This job requires SOURCE_FILTER");
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      Filter filter = RecordUtils.toRecordTemplate(Filter.class, sourceFilter.get());

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
