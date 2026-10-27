package com.linkedin.metadata.graph.inmemory;

import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.query.filter.Filter;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A graph service that can answer the lightning lineage search next to the graph: walk, filter,
 * page and count facets, returning only the page instead of every relationship.
 *
 * <p>The answer must be exactly what {@code LineageSearchService} computes from the full walk, so
 * this is only asked for requests the lightning path could serve (a {@code *} query, no sort,
 * filters only on {@link com.linkedin.metadata.search.LightningUrnFields#FILTER_FIELDS}).
 */
public interface ImpactLineageSearchPushdown {

  /**
   * One lightning search, as {@code LineageSearchService} would run it.
   *
   * @param entityTypes entity types to return; empty means all
   * @param degreeValues values of the degree filter ({@code 1}, {@code 2}, {@code 3+}); empty means
   *     no degree filter
   * @param filters the filters without their degree criteria
   * @param convertSchemaFields report schema fields as the dataset they belong to
   * @param forceLightning answer from the graph even below the lightning threshold
   * @param lightningThreshold more relationships than this (after the entity type and degree
   *     filters) are searched from the graph; fewer are returned in full for the entity index
   * @param size page size, already capped
   */
  record Spec(
      Set<String> entityTypes,
      List<String> degreeValues,
      @Nullable Filter filters,
      boolean convertSchemaFields,
      boolean forceLightning,
      long lightningThreshold,
      int from,
      int size) {}

  /** What the server answered. */
  sealed interface Outcome permits Lightning, Full {}

  /**
   * The page and counts of a lightning search.
   *
   * @param prefiltered relationships left after the entity type and degree filters
   * @param total relationships that match every filter
   */
  record Lightning(
      List<Row> rows,
      int walked,
      int prefiltered,
      int total,
      Facet platforms,
      Facet entityTypes,
      Facet origins,
      boolean partial)
      implements Outcome {}

  record Row(Urn entity, int degree, UrnArrayArray paths) {}

  /**
   * Facet value counts in the order the values were first met, and whether a later match counted an
   * already-seen value after the last new one (which decides whether {@link java.util.HashMap} grew
   * one more time when the counts were tallied).
   */
  record Facet(LinkedHashMap<String, Long> counts, boolean countedAfterLastNew) {}

  /** Too few relationships for the lightning path: the full walk, for the entity index search. */
  record Full(EntityLineageResult result) implements Outcome {}

  /**
   * Runs the walk and the lightning search on the server, or returns empty when the server cannot
   * be used for it now (the caller then walks and searches itself).
   */
  @Nonnull
  Optional<Outcome> searchImpactLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters graphFilters,
      int maxHops,
      @Nonnull Spec spec);
}
