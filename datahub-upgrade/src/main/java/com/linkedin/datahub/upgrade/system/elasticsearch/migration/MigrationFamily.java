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

  /** Why the family cannot migrate on this deployment; null when it can. */
  @Nullable private final String unsupportedReason;

  private MigrationFamily(
      @Nonnull final SearchComponent family,
      @Nullable final MigratableIndexed service,
      @Nullable final String unsupportedReason) {
    this.family = family;
    this.service = service;
    this.unsupportedReason = unsupportedReason;
  }

  /**
   * Every family is constructed whatever backs it, so the upgrade comes up on a deployment that
   * cannot migrate one of them (a Neo4j graph); only a migration configured for such a family
   * fails, see {@link #checkSupported()}.
   */
  @Nonnull
  public static MigrationFamily of(
      @Nonnull final SearchComponent family,
      @Nonnull final GraphService graphService,
      @Nonnull final SystemMetadataService systemMetadataService,
      @Nonnull final TimeseriesAspectService timeseriesAspectService) {
    switch (family) {
      case GRAPH:
        return backedBy(family, graphService);
      case SYSTEM_METADATA:
        return backedBy(family, systemMetadataService);
      case TIMESERIES:
        return backedBy(family, timeseriesAspectService);
      case USAGE:
        return new MigrationFamily(family, null, null);
      default:
        throw new IllegalArgumentException(family + " is not a migratable family");
    }
  }

  @Nonnull
  private static MigrationFamily backedBy(
      @Nonnull final SearchComponent family, @Nonnull final Object service) {
    if (service instanceof MigratableIndexed) {
      return new MigrationFamily(family, (MigratableIndexed) service, null);
    }
    return new MigrationFamily(
        family,
        null,
        service.getClass().getSimpleName()
            + " cannot build its indices on another cluster; only Elasticsearch/OpenSearch"
            + " families migrate");
  }

  @Nonnull
  public SearchComponent getFamily() {
    return family;
  }

  /** Whether this deployment's backend for the family can migrate at all. */
  public boolean isSupported() {
    return unsupportedReason == null;
  }

  /**
   * @throws IllegalStateException when this deployment's backend for the family cannot migrate
   */
  public void checkSupported() {
    if (unsupportedReason != null) {
      throw new IllegalStateException(family.getConfigKey() + ": " + unsupportedReason);
    }
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

  /**
   * The query that selects the documents written at or after {@code from}, and at or before {@code
   * to} when that is given.
   */
  @Nonnull
  public ObjectNode windowFilter(
      @Nonnull final ObjectMapper mapper, final long from, @Nullable final Long to) {
    final List<String> fields = windowFields();
    if (fields.size() == 1) {
      final ObjectNode range = mapper.createObjectNode();
      bounds(range.putObject("range").putObject(fields.get(0)), from, to);
      return range;
    }
    final ObjectNode filter = mapper.createObjectNode();
    final ObjectNode bool = filter.putObject("bool");
    final ArrayNode should = bool.putArray("should");
    for (String field : fields) {
      bounds(should.addObject().putObject("range").putObject(field), from, to);
    }
    bool.put("minimum_should_match", 1);
    return filter;
  }

  private static void bounds(
      @Nonnull final ObjectNode range, final long from, @Nullable final Long to) {
    range.put("gte", from);
    if (to != null) {
      range.put("lte", to);
    }
  }

  /**
   * Whether a document can change after it is written, so a windowed pass must replace rather than
   * create. Graph edges and system metadata rows are updated in place; a timeseries document keyed
   * by a time bucket (usage statistics, a profile with an explicit timestamp) is upserted again
   * when its bucket is re-ingested. Usage events alone are inserted once under a new id.
   */
  public boolean isMutable() {
    return family != SearchComponent.USAGE;
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
