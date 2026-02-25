package com.linkedin.datahub.upgrade.system.elasticsearch;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesPostStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesPreStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.ReindexDebugStep;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//  This is useful for debugging or special situations to reindex, the index to reindex is given in
// -u index= argument
//  For now will only work with entity, v2 indices
//  see runReindexDebug gradle task
public class ReindexDebug implements Upgrade {
  private final List<UpgradeStep> _steps;

  public ReindexDebug(
      SystemMetadataService systemMetadataService,
      TimeseriesAspectService timeseriesAspectService,
      EntitySearchService entitySearchService,
      GraphService graphService,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents baseElasticSearchComponents,
      ConfigurationProvider configurationProvider,
      AspectDao aspectDao) {
    List<ElasticSearchIndexed> indexedServices =
        Stream.of(graphService, entitySearchService, systemMetadataService, timeseriesAspectService)
            .filter(service -> service instanceof ElasticSearchIndexed)
            .map(service -> (ElasticSearchIndexed) service)
            .collect(Collectors.toList());
    _steps =
        buildSteps(indexedServices, baseElasticSearchComponents, configurationProvider, aspectDao);
  }

  @Override
  public String id() {
    return "ReindexDebug";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(
      final List<ElasticSearchIndexed> indexedServices,
      final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
          baseElasticSearchComponents,
      final ConfigurationProvider configurationProvider,
      final AspectDao aspectDao) {
    final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties = new HashSet<>();
    final List<UpgradeStep> steps = new ArrayList<>();
    // Disable ES write mode/change refresh rate and clone indices
    steps.add(
        new BuildIndicesPreStep(
            baseElasticSearchComponents,
            indexedServices,
            configurationProvider,
            structuredProperties));
    steps.add(new ReindexDebugStep(indexedServices, structuredProperties));
    steps.add(
        new BuildIndicesPostStep(
            baseElasticSearchComponents, indexedServices, structuredProperties));
    return steps;
  }
}
