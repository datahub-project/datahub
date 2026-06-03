package io.datahub.ownership.access;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Computes, per actor, the set of Domain and Data Platform URNs the actor can "see" — defined as
 * those that contain at least one asset the actor is allowed to view (owned by the actor or one of
 * their groups, OR unowned). It does this with two aggregations over the asset index using the same
 * ownership predicate the search filter uses, so the rule is consistent everywhere.
 *
 * <p>Results are cached per actor with a short TTL to keep the cost off the hot path.
 */
public class DomainPlatformAccessResolver {

    private static final String OWNERS_FIELD = "owners";
    private static final String DOMAINS_FIELD = "domains";
    private static final String PLATFORM_FIELD = "platform";
    private static final int AGG_LIMIT = 10_000;

    /**
     * Asset entity types that carry a {@code domains} and/or {@code platform} field. We aggregate
     * only over these (never the full index set) so the ownership predicate stays meaningful. The
     * list is intersected with the live registry, so unknown names on a given version are skipped.
     */
    private static final List<String> ASSET_ENTITY_CANDIDATES = List.of(
            "dataset", "dashboard", "chart", "dataFlow", "dataJob", "dataProcessInstance",
            "mlModel", "mlModelGroup", "mlFeatureTable", "mlFeature", "mlPrimaryKey",
            "container", "notebook", "dataProduct");

    public record AccessSets(Set<String> domains, Set<String> platforms) {}

    private final EntitySearchService entitySearchService;
    private final Cache<String, AccessSets> cache;

    public DomainPlatformAccessResolver(@Nonnull EntitySearchService entitySearchService) {
        this(entitySearchService, Ticker.systemTicker(), 60, TimeUnit.SECONDS);
    }

    public DomainPlatformAccessResolver(
            @Nonnull EntitySearchService entitySearchService,
            @Nonnull Ticker ticker,
            long expireAmount,
            @Nonnull TimeUnit expireUnit) {
        this.entitySearchService = entitySearchService;
        this.cache = Caffeine.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(expireAmount, expireUnit)
                .maximumSize(50_000)
                .build();
    }

    @Nonnull
    public AccessSets resolve(
            @Nonnull OperationContext opCtx, @Nonnull Urn actor, @Nonnull Collection<Urn> groups) {
        AccessSets cached = cache.getIfPresent(actor.toString());
        if (cached != null) {
            return cached;
        }
        Filter ownership = buildOwnershipFilter(actor, groups);
        // Scope the aggregation to concrete asset entity types. Passing null makes aggregateByValue
        // query EVERY entity index (getAllEntityIndicesPatterns) while the ownership filter is only
        // transformed for the default specs; on indices without an `owners` field the
        // `owners EXISTS negated` clause then matches every document, so the aggregation returns all
        // domains/platforms regardless of ownership (DataHub v1.5.x; on other ES/OpenSearch builds
        // the same query can instead return empty or throw, blanking the home-page cards).
        List<String> assetEntities = assetEntityNames(opCtx);
        Set<String> domains =
                entitySearchService.aggregateByValue(opCtx, assetEntities, DOMAINS_FIELD, ownership, AGG_LIMIT).keySet();
        Set<String> platforms =
                entitySearchService.aggregateByValue(opCtx, assetEntities, PLATFORM_FIELD, ownership, AGG_LIMIT).keySet();
        AccessSets sets = new AccessSets(Set.copyOf(domains), Set.copyOf(platforms));
        cache.put(actor.toString(), sets);
        return sets;
    }

    /** Candidate asset entity names that actually exist in this deployment's registry. */
    private List<String> assetEntityNames(@Nonnull OperationContext opCtx) {
        Set<String> known = opCtx.getEntityRegistry().getEntitySpecs().keySet();
        List<String> names = new ArrayList<>(ASSET_ENTITY_CANDIDATES.size());
        for (String name : ASSET_ENTITY_CANDIDATES) {
            if (known.contains(name)) {
                names.add(name);
            }
        }
        // Never fall back to null/empty (that would query all indices and over-match); dataset is
        // always present in a DataHub registry.
        if (names.isEmpty()) {
            names.add("dataset");
        }
        return names;
    }

    /** owners IN [actor, groups]  OR  owners absent/empty (the same predicate the search filter uses). */
    private Filter buildOwnershipFilter(Urn actor, Collection<Urn> groups) {
        List<String> ownerValues = new ArrayList<>(1 + groups.size());
        ownerValues.add(actor.toString());
        for (Urn g : groups) {
            ownerValues.add(g.toString());
        }
        Criterion ownerMatch = new Criterion()
                .setField(OWNERS_FIELD)
                .setCondition(Condition.EQUAL)
                .setValues(new StringArray(ownerValues));
        Criterion noOwners = new Criterion()
                .setField(OWNERS_FIELD)
                .setCondition(Condition.EXISTS)
                .setNegated(true);
        return new Filter().setOr(new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(ownerMatch)),
                new ConjunctiveCriterion().setAnd(new CriterionArray(noOwners))));
    }
}
