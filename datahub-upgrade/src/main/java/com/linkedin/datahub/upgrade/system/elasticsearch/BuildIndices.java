package com.linkedin.datahub.upgrade.system.elasticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.shared.ElasticSearchUpgradeUtils;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesPostStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesPreStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesStep;
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
import java.util.List;
import java.util.Set;

public class BuildIndices implements BlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public BuildIndices(
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
          baseElasticSearchComponents,
      final ConfigurationProvider configurationProvider,
      final AspectDao aspectDao) {

    List<ElasticSearchIndexed> indexedServices =
        ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
            graphService, entitySearchService, systemMetadataService, timeseriesAspectService);

    _steps =
        buildSteps(indexedServices, baseElasticSearchComponents, configurationProvider, aspectDao);
  }

  @Override
  public String id() {
    return "BuildIndices";
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

    final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
    if (configurationProvider.getStructuredProperties().isSystemUpdateEnabled()) {
      structuredProperties =
          ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(aspectDao);
    } else {
      structuredProperties = Set.of();
    }

    final List<UpgradeStep> steps = new ArrayList<>();
    // Disable ES write mode/change refresh rate and clone indices
    steps.add(
        new BuildIndicesPreStep(
            baseElasticSearchComponents,
            indexedServices,
            configurationProvider,
            structuredProperties));
    // Configure graphService, entitySearchService, systemMetadataService, timeseriesAspectService
    steps.add(new BuildIndicesStep(indexedServices, structuredProperties));
    // Reset configuration (and delete clones? Or just do this regularly? Or delete clone in
    // pre-configure step if it already exists?
    steps.add(
        new BuildIndicesPostStep(
            baseElasticSearchComponents, indexedServices, structuredProperties));
    return steps;
  }
}
