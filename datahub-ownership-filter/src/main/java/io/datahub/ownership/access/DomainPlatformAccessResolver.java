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
        // entityNames = null -> aggregate across all entity types; the field only exists on assets.
        Set<String> domains =
                entitySearchService.aggregateByValue(opCtx, null, DOMAINS_FIELD, ownership, AGG_LIMIT).keySet();
        Set<String> platforms =
                entitySearchService.aggregateByValue(opCtx, null, PLATFORM_FIELD, ownership, AGG_LIMIT).keySet();
        AccessSets sets = new AccessSets(Set.copyOf(domains), Set.copyOf(platforms));
        cache.put(actor.toString(), sets);
        return sets;
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
