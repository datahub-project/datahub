package com.linkedin.datahub.upgrade.propagate;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class PropagateTermsStep implements UpgradeStep {

  private final EntityFetcher _entityFetcher;
  private final EntitySearchService _entitySearchService;

  public PropagateTermsStep(EntityService entityService, EntitySearchService entitySearchService) {
    _entityFetcher = new EntityFetcher(entityService);
    _entitySearchService = entitySearchService;
  }

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

//      Optional<String> sourceFilter = context.parsedArgs().get("SOURCE_FILTER");
//      if (!sourceFilter.isPresent()) {
//        context.report().addLine("Missing required arguments. This job requires SOURCE_FILTER");
//        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
//      }
      Filter sourceFilter = QueryUtils.newFilter("platform.keyword", "urn:li:dataPlatform:mysql");

      context.report().addLine("Fetching source entities to propagate from");

      SearchResult sourceSearchResults =
          _entitySearchService.filter(Constants.DATASET_ENTITY_NAME, sourceFilter, null, 0, 5000);

      context.report().addLine(String.format("Found %d source entities", sourceSearchResults.getNumEntities()));
      context.report().addLine("Fetching schema for the source entities");

      Map<Urn, EntityFetcher.EntityDetails> sourceEntityDetails = _entityFetcher.fetchSchema(
          sourceSearchResults.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toSet()));

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
