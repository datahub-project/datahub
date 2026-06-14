package io.datahub.ownership.filter;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the ownership predicate as a list of GraphQL {@code AndFilterInput} disjuncts
 * (an OR-of-ANDs). The predicate an actor is allowed to see is:
 *
 * <pre>
 *   (owners contains the actor OR one of their groups)   // they own it
 *   OR
 *   (owners field is absent or empty)                    // nobody owns it -> visible to everyone
 * </pre>
 *
 * The second disjunct implements the rule "assets with no ownership assigned are visible to all
 * users". It uses the {@code EXISTS} operator negated, which DataHub evaluates as
 * "field is not present or is an empty array".
 */
public class OwnershipFilterBuilder {

    private static final String OWNERS_FIELD = "owners";

    @Nonnull
    public List<Map<String, Object>> injectOwnershipFilter(
            @Nonnull String actorUrn,
            @Nonnull Collection<String> groupUrns) {

        List<Map<String, Object>> disjuncts = new ArrayList<>(2);
        // Disjunct 1: the actor or one of their groups is an owner.
        disjuncts.add(andOf(ownerMatchCriterion(actorUrn, groupUrns)));
        // Disjunct 2: the entity has no owners at all -> visible to everyone.
        disjuncts.add(andOf(noOwnersCriterion()));
        return disjuncts;
    }

    /**
     * The single criterion "owners contains one of [actor, groups]". Kept package-visible so the
     * field-argument mutator can reuse just this criterion for flat-filter fields (e.g. legacy
     * browse) that cannot express the OR with the no-owners disjunct.
     */
    Map<String, Object> ownerMatchCriterion(String actor, Collection<String> groups) {
        List<String> values = new ArrayList<>(1 + groups.size());
        values.add(actor);
        values.addAll(groups);
        Map<String, Object> facet = new LinkedHashMap<>();
        facet.put("field", OWNERS_FIELD);
        facet.put("condition", "EQUAL");
        facet.put("values", values);
        return facet;
    }

    private Map<String, Object> noOwnersCriterion() {
        Map<String, Object> facet = new LinkedHashMap<>();
        facet.put("field", OWNERS_FIELD);
        facet.put("condition", "EXISTS");
        facet.put("negated", true);
        return facet;
    }

    private Map<String, Object> andOf(Map<String, Object> criterion) {
        Map<String, Object> disjunct = new LinkedHashMap<>();
        disjunct.put("and", new ArrayList<>(List.of(criterion)));
        return disjunct;
    }
}
