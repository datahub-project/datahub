package com.linkedin.datahub.upgrade.shared;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;

import com.datahub.util.RecordUtils;
import com.google.common.base.Suppliers;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.GetMode;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared utilities for Elasticsearch upgrade operations. Contains common functionality used by both
 * BuildIndices and LoadIndices upgrades.
 */
@Slf4j
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
   * Structured property definitions to build index mapping targets from: the active definitions
   * whenever structured properties are enabled, so a reindex converges each index to them. {@code
   * systemUpdateEnabled} does not gate this; it only decides whether a structured-property
   * difference may trigger a reindex.
   *
   * <p>Nothing is read until the first {@code get()}, and a successful read is kept. Upgrades are
   * built while Spring starts, so most call {@code get()} from the step that needs the definitions:
   * a slow or failed read (on Cassandra each one is a filtering scan of the aspect table) stays
   * inside that step, and those upgrades read nothing unless they run. BuildIndices is the
   * exception: it calls it when it is built, because its scale-down vote would count a failed read
   * as no reindex due.
   *
   * @param structuredPropertiesConfiguration structured properties configuration
   * @param aspectDao the aspect DAO for database access
   * @return active structured property definitions, or none when structured properties are disabled
   */
  public static Supplier<Set<Pair<Urn, StructuredPropertyDefinition>>>
      getStructuredPropertiesForIndexMappings(
          StructuredPropertiesConfiguration structuredPropertiesConfiguration,
          AspectDao aspectDao) {
    if (!structuredPropertiesConfiguration.isEnabled()) {
      return Set::of;
    }
    return Suppliers.memoize(() -> getActiveStructuredPropertiesDefinitions(aspectDao));
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
    // On Cassandra each read below scans the aspect table, so its cost shows here.
    long startMillis = System.currentTimeMillis();

    // First, get all removed structured property URNs
    Set<String> removedStructuredPropertyUrns;
    try (Stream<EntityAspect> stream =
        aspectDao.streamAspects(STRUCTURED_PROPERTY_ENTITY_NAME, STATUS_ASPECT_NAME)) {
      removedStructuredPropertyUrns =
          stream
              .filter(ElasticSearchUpgradeUtils::isRemoved)
              .map(EntityAspect::getUrn)
              .collect(Collectors.toSet());
    }

    // Then, get all structured property definitions and filter out removed ones
    Set<Pair<Urn, StructuredPropertyDefinition>> definitions;
    try (Stream<EntityAspect> stream =
        aspectDao.streamAspects(
            STRUCTURED_PROPERTY_ENTITY_NAME, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)) {
      definitions =
          stream
              .map(ElasticSearchUpgradeUtils::toMappableDefinition)
              .flatMap(Optional::stream)
              .filter(
                  definition ->
                      !removedStructuredPropertyUrns.contains(definition.getKey().toString()))
              .collect(Collectors.toSet());
    }
    log.info(
        "Read {} active structured property definitions in {} ms",
        definitions.size(),
        System.currentTimeMillis() - startMillis);
    return definitions;
  }

  // An unreadable status keeps its property: leaving an active property out of the mappings would
  // make its values unsearchable, while keeping a removed one's fields is harmless.
  private static boolean isRemoved(EntityAspect statusAspect) {
    try {
      return RecordUtils.toRecordTemplate(Status.class, statusAspect.getMetadata()).isRemoved();
    } catch (RuntimeException e) {
      log.error(
          "Treating structured property {} as active, its status is unreadable",
          statusAspect.getUrn(),
          e);
      return false;
    }
  }

  /**
   * The definition in {@code entityAspect}, or empty when it cannot be read, lacks a required field
   * the mapping builders read, or names no valid search field (a qualified name with a space,
   * allowed before write validation existed). Index mappings are built from every definition at
   * once, so one such definition would otherwise fail the build of every index.
   */
  private static Optional<Pair<Urn, StructuredPropertyDefinition>> toMappableDefinition(
      EntityAspect entityAspect) {
    try {
      Urn urn = UrnUtils.getUrn(entityAspect.getUrn());
      StructuredPropertyDefinition definition =
          RecordUtils.toRecordTemplate(
              StructuredPropertyDefinition.class, entityAspect.getMetadata());
      if (definition.getEntityTypes(GetMode.NULL) == null
          || definition.getValueType(GetMode.NULL) == null) {
        log.error(
            "Leaving structured property {} out of the index mappings: its definition has no"
                + " entityTypes or valueType",
            urn);
        return Optional.empty();
      }
      StructuredPropertyUtils.toElasticsearchFieldName(urn, definition);
      return Optional.of(Pair.of(urn, definition));
    } catch (RuntimeException e) {
      log.error(
          "Leaving structured property {} out of the index mappings", entityAspect.getUrn(), e);
      return Optional.empty();
    }
  }
}
