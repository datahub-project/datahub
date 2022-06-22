package com.linkedin.datahub.upgrade.test;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.test.TestResults;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class EvaluateTestsStep implements UpgradeStep {

  private final EntitySearchService _entitySearchService;
  private final TestEngine _testEngine;

  @Override
  public String id() {
    return "EvaluateTests";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      context.report().addLine("Starting to evaluate tests...");

      int batchSize =
          context.parsedArgs().getOrDefault("BATCH_SIZE", Optional.empty()).map(Integer::parseInt).orElse(1000);

      Set<String> entityTypesToEvaluate = _testEngine.getEntityTypesToEvaluate();
      context.report().addLine(String.format("Evaluating tests for entities %s", entityTypesToEvaluate));

      for (String entityType : entityTypesToEvaluate) {
        int batch = 1;
        context.report().addLine(String.format("Fetching batch %d of %s entities", batch, entityType));
        ScrollResult scrollResult = _entitySearchService.scroll(entityType, null, null, batchSize, null, "1m");
        while (scrollResult.getEntities().size() > 0) {
          context.report().addLine(String.format("Processing batch %d of %s entities", batch, entityType));
          List<Urn> entitiesInBatch =
              scrollResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());
          Map<Urn, TestResults> result;
          try {
            result = _testEngine.batchEvaluateTestsForEntities(entitiesInBatch, true);
            context.report()
                .addLine(String.format("Pushed %d test results for batch %d of %s entities", result.size(), batch,
                    entityType));
          } catch (Exception e) {
            context.report().addLine(String.format("Error while processing batch %d of %s entities", batch, entityType));
            log.error("Error while processing batch {} of {} entities", batch, entityType, e);
          }
          batch++;
          context.report().addLine(String.format("Fetching batch %d of %s entities", batch, entityType));
          scrollResult =
              _entitySearchService.scroll(entityType, null, null, batchSize, scrollResult.getScrollId(), "1m");
        }
        context.report().addLine(String.format("Finished evaluating tests for %s entities", entityType));
      }

      context.report().addLine("Finished evaluating tests for all entities");

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
