package com.linkedin.datahub.upgrade.shared;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
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

/**
 * Shared utilities for Elasticsearch upgrade operations. Contains common functionality used by both
 * BuildIndices and LoadIndices upgrades.
 */
public class ElasticSearchUpgradeUtils {

  /**
   * Creates a list of ElasticSearchIndexed services from the provided services. Filters out
   * services that don't implement ElasticSearchIndexed.
   *
   * @param graphService the graph service
   * @param entitySearchService the entity search service
   * @param systemMetadataService the system metadata service
   * @param timeseriesAspectService the timeseries aspect service
   * @return list of ElasticSearchIndexed services
   */
  public static List<ElasticSearchIndexed> createElasticSearchIndexedServices(
      GraphService graphService,
      EntitySearchService entitySearchService,
      SystemMetadataService systemMetadataService,
      TimeseriesAspectService timeseriesAspectService) {

    return Stream.of(
            graphService, entitySearchService, systemMetadataService, timeseriesAspectService)
        .filter(service -> service instanceof ElasticSearchIndexed)
        .map(service -> (ElasticSearchIndexed) service)
        .collect(Collectors.toList());
  }

  /**
   * Gets active structured properties definitions from the database. This method retrieves all
   * structured property definitions that are not marked as removed.
   *
   * @param aspectDao the aspect DAO for database access
   * @return set of active structured property definitions
   */
  public static Set<Pair<Urn, StructuredPropertyDefinition>>
      getActiveStructuredPropertiesDefinitions(AspectDao aspectDao) {

    // First, get all removed structured property URNs
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

    // Then, get all structured property definitions and filter out removed ones
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
