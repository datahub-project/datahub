package com.linkedin.datahub.upgrade.system.elasticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.shared.ElasticSearchUpgradeUtils;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesPostStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesPreStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.CreateUsageEventIndicesStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.CreateUserStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildIndices implements BlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;
  private final List<ElasticSearchIndexed> _indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> _structuredProperties;

  public BuildIndices(
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
          baseElasticSearchComponents,
      final ConfigurationProvider configurationProvider,
      final AspectDao aspectDao) {

    _indexedServices =
        ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
            graphService, entitySearchService, systemMetadataService, timeseriesAspectService);

    if (configurationProvider.getStructuredProperties().isSystemUpdateEnabled()) {
      _structuredProperties =
          ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(aspectDao);
    } else {
      _structuredProperties = Set.of();
    }

    _steps =
        buildSteps(_indexedServices, baseElasticSearchComponents, configurationProvider, aspectDao);
  }

  @Override
  public boolean requiresK8ScaleDown(UpgradeContext context) {
    try {
      List<ReindexConfig> configs =
          IndexUtils.getAllReindexConfigs(
              context.opContext(), _indexedServices, _structuredProperties);
      return configs.stream().anyMatch(ReindexConfig::requiresReindex);
    } catch (IOException e) {
      log.warn("Could not evaluate reindex configs for scale-down vote: {}", e.getMessage());
      return false;
    }
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

    final List<UpgradeStep> steps = new ArrayList<>();
    // Setup Elasticsearch users and roles (if enabled)
    steps.add(new CreateUserStep(baseElasticSearchComponents, configurationProvider));
    // Setup usage event indices and policies
    steps.add(new CreateUsageEventIndicesStep(baseElasticSearchComponents, configurationProvider));
    // Disable ES write mode/change refresh rate and clone indices
    steps.add(
        new BuildIndicesPreStep(
            baseElasticSearchComponents,
            indexedServices,
            configurationProvider,
            _structuredProperties));
    // Configure graphService, entitySearchService, systemMetadataService, timeseriesAspectService
    steps.add(new BuildIndicesStep(indexedServices, _structuredProperties));
    // Reset configuration (and delete clones? Or just do this regularly? Or delete clone in
    // pre-configure step if it already exists?
    steps.add(
        new BuildIndicesPostStep(
            baseElasticSearchComponents, indexedServices, _structuredProperties));
    return steps;
  }
}
