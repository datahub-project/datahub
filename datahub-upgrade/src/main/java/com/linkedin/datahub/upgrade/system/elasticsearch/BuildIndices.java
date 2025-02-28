package com.linkedin.datahub.upgrade.system.elasticsearch;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesPostStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesPreStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesStep;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.aspect.EntityAspect;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        Stream.of(graphService, entitySearchService, systemMetadataService, timeseriesAspectService)
            .filter(service -> service instanceof ElasticSearchIndexed)
            .map(service -> (ElasticSearchIndexed) service)
            .collect(Collectors.toList());

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
      structuredProperties = getActiveStructuredPropertiesDefinitions(aspectDao);
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

  static Set<Pair<Urn, StructuredPropertyDefinition>> getActiveStructuredPropertiesDefinitions(
      AspectDao aspectDao) {
    Set<String> removedStructuredPropertyUrns;
    try (Stream<EntityAspect> stream =
        aspectDao.streamAspects(STRUCTURED_PROPERTY_ENTITY_NAME, STATUS_ASPECT_NAME)) {
      removedStructuredPropertyUrns =
          stream
              .map(
                  entityAspect ->
                      Pair.of(
                          entityAspect.getUrn(),
                          RecordUtils.toRecordTemplate(Status.class, entityAspect.getMetadata())))
              .filter(status -> status.getSecond().isRemoved())
              .map(Pair::getFirst)
              .collect(Collectors.toSet());
    }

    try (Stream<EntityAspect> stream =
        aspectDao.streamAspects(
            STRUCTURED_PROPERTY_ENTITY_NAME, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)) {
      return stream
          .map(
              entityAspect ->
                  Pair.of(
                      UrnUtils.getUrn(entityAspect.getUrn()),
                      RecordUtils.toRecordTemplate(
                          StructuredPropertyDefinition.class, entityAspect.getMetadata())))
          .filter(
              definition -> !removedStructuredPropertyUrns.contains(definition.getKey().toString()))
          .collect(Collectors.toSet());
    }
  }
}
