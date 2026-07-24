package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * Per-request DataLoader resolving {@code Domain.entities.total} for the count-only selections in
 * the {@code domainEntitiesFields} fragment ({@code entities}/{@code dataProducts}/{@code
 * applicationsInDomain}), collapsing the previous N&times;3 {@code searchAcrossEntities} calls into
 * a fixed number of aggregation-only searches.
 *
 * <p>Each key is a (domain, entity-type constraint) pair. Keys are grouped by their constraint, and
 * each group issues one aggregation-only search per chunk of domains, faceted on {@code domains}
 * and filtered by the group's {@code _entityType} criterion. The {@code domains} facet buckets are
 * keyed by domain urn, so the per-domain totals read back unambiguously &mdash; the entity-type
 * constraint is expressed as a search filter (exactly as the pre-batch resolver did), never as an
 * aggregation dimension, so there is no dependence on the entity-type token format the search layer
 * emits.
 *
 * <p><b>Bucket-cap caveat.</b> Like {@code GlossaryTreeCountsBatchLoader}, this relies on a terms
 * aggregation whose bucket count is capped at {@code min(maxAggValues, maxTermBucketSize)}. Because
 * the query is already filtered to the chunk's domains, the {@code domains} bucket only needs to
 * hold those domains (plus any off-chunk domains contributed by multi-domain assets, which are
 * ignored). We chunk conservatively ({@link #MAX_DOMAINS_PER_AGG}) and raise {@code maxAggValues}.
 */
@Slf4j
public final class DomainEntityCountsBatchLoader {

  public static final String LOADER_NAME = "DomainEntityCounts";

  private static final String DOMAINS_FIELD = "domains";
  private static final String DOMAINS_KEYWORD_FIELD = DOMAINS_FIELD + ".keyword";
  private static final String MATCH_ALL_QUERY = "*";

  // Keep chunks safely under the search-layer terms-bucket cap (default maxTermBucketSize == 60),
  // leaving headroom for off-chunk domains pulled in by multi-domain assets.
  private static final int MAX_DOMAINS_PER_AGG = 25;
  private static final int MAX_AGG_VALUES = 1000;

  private DomainEntityCountsBatchLoader() {}

  public static DataLoader<DomainCountKey, Long> create(
      final EntityClient entityClient, final QueryContext queryContext) {
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);

    // Parent the batchLoad span under the operation, not the executor thread (see
    // GlossaryTreeCountsBatchLoader / EntityAspectsDataLoader).
    final Context batchContext = Context.current();

    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try (Scope ignored = batchContext.makeCurrent()) {
                    return batchLoad(keys, (QueryContext) env.getContext(), entityClient);
                  }
                },
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  @WithSpan
  public static List<Long> batchLoad(
      final List<DomainCountKey> keys,
      final QueryContext queryContext,
      final EntityClient entityClient) {
    // key -> total, defaulting to zero (a domain with no visible matching assets stays zero).
    final Map<DomainCountKey, Long> resultByKey = new HashMap<>(keys.size());
    for (DomainCountKey key : keys) {
      resultByKey.put(key, 0L);
    }

    final List<String> entityNames =
        SEARCHABLE_ENTITY_TYPES.stream()
            .map(EntityTypeMapper::getName)
            .collect(Collectors.toList());

    // Group by the entity-type constraint: all keys sharing a constraint are answered by the same
    // aggregation(s), one per chunk of their domains.
    final Map<EntityTypeConstraint, List<DomainCountKey>> byConstraint = new LinkedHashMap<>();
    for (DomainCountKey key : keys) {
      byConstraint.computeIfAbsent(key.constraint(), k -> new ArrayList<>()).add(key);
    }

    for (Map.Entry<EntityTypeConstraint, List<DomainCountKey>> group : byConstraint.entrySet()) {
      final EntityTypeConstraint constraint = group.getKey();
      final List<String> domainUrns =
          group.getValue().stream()
              .map(DomainCountKey::getDomainUrn)
              .distinct()
              .collect(Collectors.toList());
      for (List<String> chunk : partition(domainUrns, MAX_DOMAINS_PER_AGG)) {
        try {
          final Map<String, Long> countsByDomain =
              loadChunk(queryContext, entityClient, entityNames, constraint, chunk);
          for (DomainCountKey key : group.getValue()) {
            final Long count = countsByDomain.get(key.getDomainUrn());
            if (count != null) {
              resultByKey.put(key, count);
            }
          }
        } catch (Exception e) {
          log.error(
              "Failed to batch-load domain entity counts for {} domains (constraint {})",
              chunk.size(),
              constraint,
              e);
        }
      }
    }

    // DataLoader contract: results[i] must correspond to keys[i].
    final List<Long> ordered = new ArrayList<>(keys.size());
    for (DomainCountKey key : keys) {
      ordered.add(resultByKey.get(key));
    }
    return ordered;
  }

  private static Map<String, Long> loadChunk(
      final QueryContext queryContext,
      final EntityClient entityClient,
      final List<String> entityNames,
      final EntityTypeConstraint constraint,
      final List<String> domainUrns)
      throws Exception {

    final OperationContext opContext =
        queryContext
            .getOperationContext()
            .withSearchFlags(
                flags -> flags.setMaxAggValues(MAX_AGG_VALUES).setIncludeDefaultFacets(false));

    final SearchResult searchResult =
        entityClient.searchAcrossEntities(
            opContext,
            entityNames,
            MATCH_ALL_QUERY,
            buildFilter(domainUrns, constraint),
            0,
            0,
            Collections.emptyList(),
            Collections.singletonList(DOMAINS_FIELD));

    final Map<String, Long> countsByDomain = new HashMap<>();
    if (searchResult == null
        || searchResult.getMetadata() == null
        || searchResult.getMetadata().getAggregations() == null) {
      return countsByDomain;
    }
    for (AggregationMetadata agg : searchResult.getMetadata().getAggregations()) {
      if (!DOMAINS_FIELD.equals(agg.getName()) || agg.getAggregations() == null) {
        continue;
      }
      // domains-facet bucket keys are domain urns; ignore off-chunk domains from multi-domain
      // assets.
      agg.getAggregations()
          .forEach(
              (domainUrn, count) -> {
                if (domainUrns.contains(domainUrn)) {
                  countsByDomain.merge(domainUrn, count, Long::sum);
                }
              });
    }
    return countsByDomain;
  }

  private static Filter buildFilter(
      final List<String> domainUrns, final EntityTypeConstraint constraint) {
    final StringArray urnValues = new StringArray();
    for (String urn : domainUrns) {
      try {
        urnValues.add(UrnUtils.getUrn(urn).toString());
      } catch (Exception e) {
        log.warn("Skipping malformed domain urn '{}' in domain entity count batch.", urn, e);
      }
    }

    final CriterionArray criteria = new CriterionArray();
    criteria.add(buildCriterion(DOMAINS_KEYWORD_FIELD, Condition.EQUAL, urnValues));
    if (!constraint.entityTypeNames.isEmpty()) {
      criteria.add(
          buildCriterion(
              INDEX_VIRTUAL_FIELD,
              Condition.EQUAL,
              constraint.negated,
              constraint.entityTypeNames));
    }
    return new Filter()
        .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(criteria)));
  }

  private static <T> List<List<T>> partition(final List<T> list, final int size) {
    final List<List<T>> chunks = new ArrayList<>();
    for (int i = 0; i < list.size(); i += size) {
      chunks.add(list.subList(i, Math.min(i + size, list.size())));
    }
    return chunks;
  }

  /**
   * The entity-type portion of a count request. Empty {@code entityTypeNames} means no entity-type
   * constraint (grand total across all searchable types). The names are GraphQL {@code EntityType}
   * enum values (e.g. {@code DATA_PRODUCT}) applied to the {@code _entityType} search field &mdash;
   * the same values the {@code domainEntitiesFields} filters use.
   */
  private static final class EntityTypeConstraint {
    private final List<String> entityTypeNames;
    private final boolean negated;

    EntityTypeConstraint(final List<String> entityTypeNames, final boolean negated) {
      this.entityTypeNames = entityTypeNames;
      this.negated = negated;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof EntityTypeConstraint)) {
        return false;
      }
      final EntityTypeConstraint that = (EntityTypeConstraint) o;
      return negated == that.negated && entityTypeNames.equals(that.entityTypeNames);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entityTypeNames, negated);
    }

    @Override
    public String toString() {
      return (negated ? "NOT " : "") + entityTypeNames;
    }
  }

  /**
   * Loader key: a domain plus the entity-type constraint to count within it. Two aliases on the
   * same domain with different constraints are distinct keys; aliases sharing a constraint across
   * domains are answered by one aggregation.
   */
  public static final class DomainCountKey {
    private final String domainUrn;
    private final List<String> entityTypeNames;
    private final boolean negated;

    /** No entity-type constraint: counts all searchable assets in the domain. */
    public DomainCountKey(final String domainUrn) {
      this(domainUrn, Collections.emptyList(), false);
    }

    public DomainCountKey(
        final String domainUrn, final List<String> entityTypeNames, final boolean negated) {
      this.domainUrn = domainUrn;
      this.entityTypeNames = List.copyOf(entityTypeNames);
      this.negated = negated;
    }

    public String getDomainUrn() {
      return domainUrn;
    }

    private EntityTypeConstraint constraint() {
      return new EntityTypeConstraint(entityTypeNames, negated);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DomainCountKey)) {
        return false;
      }
      final DomainCountKey that = (DomainCountKey) o;
      return negated == that.negated
          && domainUrn.equals(that.domainUrn)
          && entityTypeNames.equals(that.entityTypeNames);
    }

    @Override
    public int hashCode() {
      return Objects.hash(domainUrn, entityTypeNames, negated);
    }
  }
}
