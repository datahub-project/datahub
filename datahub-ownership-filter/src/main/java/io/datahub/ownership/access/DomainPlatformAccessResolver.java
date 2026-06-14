package io.datahub.ownership.access;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.models.EntitySpec;
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
import java.util.stream.Collectors;

/**
 * Computes, per actor, the set of Domain and Data Platform URNs the actor can "see" — defined as
 * those that contain at least one asset the actor is allowed to view (owned by the actor or one of
 * their groups, OR unowned).
 *
 * <p>It runs two filtered aggregations over the ownership predicate, scoped to the entity types that
 * can actually be owned (read live from the registry — never hardcoded, and automatically covering
 * custom entities). {@code aggregateByValue} aggregates only over docs matching the filter, so the
 * result is exactly the domains/platforms the actor can see. We must NOT use search facets here:
 * DataHub computes facets ignoring {@code postFilters} (to offer them as refinements), so a faceted
 * search would return every domain regardless of ownership. We must also NOT pass {@code null}
 * entity names: that makes aggregateByValue query every index while the filter is only transformed
 * for the default specs, and the {@code owners EXISTS negated} clause then matches all docs on
 * owner-less indices.
 *
 * <p>Results are cached per actor with a short TTL to keep the cost off the hot path.
 */
public class DomainPlatformAccessResolver {

    private static final String OWNERS_FIELD = "owners";
    private static final String DOMAINS_FIELD = "domains";
    private static final String PLATFORM_FIELD = "platform";
    // The aspect that marks an entity as assignable to a domain. Aggregating only over entities that
    // carry it scopes the predicate to real assets and excludes the domain/platform entities
    // themselves (whose own domains/platform fields would otherwise leak every value back in).
    private static final String DOMAINS_ASPECT = "domains";
    private static final int AGG_LIMIT = 10_000;

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
        List<String> assets = domainAssignableEntityNames(opCtx);
        Set<String> domains =
                entitySearchService.aggregateByValue(opCtx, assets, DOMAINS_FIELD, ownership, AGG_LIMIT).keySet();
        Set<String> platforms =
                entitySearchService.aggregateByValue(opCtx, assets, PLATFORM_FIELD, ownership, AGG_LIMIT).keySet();
        AccessSets sets = new AccessSets(Set.copyOf(domains), Set.copyOf(platforms));
        cache.put(actor.toString(), sets);
        return sets;
    }

    /**
     * Entity types that can be assigned to a domain (carry the {@code domains} aspect), read live
     * from the registry — so the scope is never hardcoded, automatically covers custom entities, and
     * excludes the domain/platform entities themselves.
     */
    private List<String> domainAssignableEntityNames(@Nonnull OperationContext opCtx) {
        return opCtx.getEntityRegistry().getEntitySpecs().values().stream()
                .filter(spec -> Boolean.TRUE.equals(spec.hasAspect(DOMAINS_ASPECT)))
                .map(EntitySpec::getName)
                .collect(Collectors.toList());
    }

    private StringArray ownerValues(Urn actor, Collection<Urn> groups) {
        List<String> vals = new ArrayList<>(1 + groups.size());
        vals.add(actor.toString());
        for (Urn g : groups) {
            vals.add(g.toString());
        }
        return new StringArray(vals);
    }

    /**
     * owners IN [actor, groups] — ownership ONLY (no unowned/EXISTS-negated disjunct).
     *
     * <p>This computes "domains/platforms where the actor or one of their groups owns an asset",
     * which is the access rule. We deliberately do NOT include the unowned disjunct here (unlike the
     * asset search filter): an unowned sub-entity that merely inherits a domain — e.g. a schemaField
     * of a DHS-owned dataset, which itself carries no owners — would otherwise be matched and would
     * leak that domain to everyone. EQUAL only matches docs the actor actually owns, so nothing
     * unowned can leak in. Unowned top-level assets remain visible in asset search via the separate
     * search filter; they just don't, on their own, grant a domain/platform to a non-owner.
     */
    private Filter buildOwnershipFilter(Urn actor, Collection<Urn> groups) {
        Criterion ownerMatch = new Criterion()
                .setField(OWNERS_FIELD)
                .setCondition(Condition.EQUAL)
                .setValues(ownerValues(actor, groups));
        return new Filter().setOr(new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(ownerMatch))));
    }
}
