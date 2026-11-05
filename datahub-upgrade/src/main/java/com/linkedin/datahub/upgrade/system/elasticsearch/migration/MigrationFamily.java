package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.shared.MigratableIndexed;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** What the migration needs to know about each non-entity family. */
public final class MigrationFamily {
  private final SearchComponent family;
  @Nullable private final MigratableIndexed service;

  private MigrationFamily(
      @Nonnull final SearchComponent family, @Nullable final MigratableIndexed service) {
    this.family = family;
    this.service = service;
  }

  @Nonnull
  public static MigrationFamily of(
      @Nonnull final SearchComponent family,
      @Nonnull final GraphService graphService,
      @Nonnull final SystemMetadataService systemMetadataService,
      @Nonnull final TimeseriesAspectService timeseriesAspectService) {
    switch (family) {
      case GRAPH:
        return new MigrationFamily(family, indexed(graphService));
      case SYSTEM_METADATA:
        return new MigrationFamily(family, indexed(systemMetadataService));
      case TIMESERIES:
        return new MigrationFamily(family, indexed(timeseriesAspectService));
      case USAGE:
        return new MigrationFamily(family, null);
      default:
        throw new IllegalArgumentException(family + " is not a migratable family");
    }
  }

  @Nonnull
  private static MigratableIndexed indexed(@Nonnull final Object service) {
    if (!(service instanceof MigratableIndexed)) {
      throw new IllegalStateException(
          service.getClass().getSimpleName()
              + " cannot build its indices on another cluster; only Elasticsearch/OpenSearch"
              + " families migrate");
    }
    return (MigratableIndexed) service;
  }

  @Nonnull
  public SearchComponent getFamily() {
    return family;
  }

  /** The service whose mappings define the family's indices; null for usage events. */
  @Nullable
  public MigratableIndexed getService() {
    return service;
  }

  /**
   * The indexed time field a document's version is judged by: the guard of the windowed pass of a
   * mutable family, and the order of the verify sample.
   */
  @Nonnull
  public String timeField() {
    switch (family) {
      case GRAPH:
        return "updatedOn";
      case SYSTEM_METADATA:
        return "lastUpdated";
      default:
        return "@timestamp";
    }
  }

  /**
   * The fields that place a document in the windowed pass: a document is in the window when any of
   * them is at or after its start. A graph edge carries {@code createdOn} and {@code updatedOn}
   * only when its relationship does, so both are looked at; an edge with neither is outside any
   * window and is covered by the copy and the mirror alone.
   */
  @Nonnull
  public List<String> windowFields() {
    return family == SearchComponent.GRAPH
        ? List.of("createdOn", "updatedOn")
        : List.of(timeField());
  }

  /** The query that selects the documents written at or after {@code windowStart}. */
  @Nonnull
  public ObjectNode windowFilter(@Nonnull final ObjectMapper mapper, final long windowStart) {
    final List<String> fields = windowFields();
    if (fields.size() == 1) {
      final ObjectNode range = mapper.createObjectNode();
      range.putObject("range").putObject(fields.get(0)).put("gte", windowStart);
      return range;
    }
    final ObjectNode filter = mapper.createObjectNode();
    final ObjectNode bool = filter.putObject("bool");
    final ArrayNode should = bool.putArray("should");
    for (String field : fields) {
      should.addObject().putObject("range").putObject(field).put("gte", windowStart);
    }
    bool.put("minimum_should_match", 1);
    return filter;
  }

  /** Whether a document can change after it is written, so a windowed pass must overwrite. */
  public boolean isMutable() {
    return family == SearchComponent.GRAPH || family == SearchComponent.SYSTEM_METADATA;
  }

  /**
   * The family's logical index names on the serving cluster, from the registry and the naming
   * convention alone: nothing here asks a cluster anything.
   */
  @Nonnull
  public List<String> logicalNames(
      @Nonnull final OperationContext opContext, @Nonnull final IndexConvention indexConvention) {
    switch (family) {
      case GRAPH:
        return List.of(
            indexConvention.getIndexName(
                opContext, SearchComponent.GRAPH, ElasticSearchGraphService.INDEX_NAME));
      case SYSTEM_METADATA:
        return List.of(
            indexConvention.getIndexName(
                opContext,
                SearchComponent.SYSTEM_METADATA,
                ElasticSearchSystemMetadataService.INDEX_NAME));
      case TIMESERIES:
        return opContext.getEntityRegistry().getEntitySpecs().values().stream()
            .flatMap(
                entitySpec ->
                    entitySpec.getAspectSpecs().stream()
                        .filter(AspectSpec::isTimeseries)
                        .map(
                            aspectSpec ->
                                indexConvention.getTimeseriesAspectIndexName(
                                    opContext, entitySpec.getName(), aspectSpec.getName())))
            .collect(Collectors.toList());
      case USAGE:
        return List.of(
            indexConvention.getIndexName(
                opContext, SearchComponent.USAGE, Constants.DATAHUB_USAGE_EVENT_INDEX));
      default:
        throw new IllegalStateException(family + " is not a migratable family");
    }
  }
}
