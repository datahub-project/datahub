package com.linkedin.datahub.upgrade.system.elasticsearch;

import static com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices.getActiveStructuredPropertiesDefinitions;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.CleanIndicesStep;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CleanIndices implements NonBlockingSystemUpgrade {
  private final List<UpgradeStep> _steps;

  public CleanIndices(
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
          baseElasticSearchComponents,
      final ConfigurationProvider configurationProvider,
      final AspectDao aspectDao) {

    final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
    if (configurationProvider.getStructuredProperties().isSystemUpdateEnabled()) {
      structuredProperties = getActiveStructuredPropertiesDefinitions(aspectDao);
    } else {
      structuredProperties = Set.of();
    }

    List<ElasticSearchIndexed> indexedServices =
        Stream.of(graphService, entitySearchService, systemMetadataService, timeseriesAspectService)
            .filter(service -> service instanceof ElasticSearchIndexed)
            .map(service -> (ElasticSearchIndexed) service)
            .collect(Collectors.toList());

    _steps =
        List.of(
            new CleanIndicesStep(
                baseElasticSearchComponents.getSearchClient(),
                configurationProvider.getElasticSearch(),
                indexedServices,
                structuredProperties));
  }

  @Override
  public String id() {
    return "CleanIndices";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
