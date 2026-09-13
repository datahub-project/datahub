package io.datahub.ownership.instrumentation;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Knows, per GraphQL field, where the ownership predicate must be injected and in what shape.
 *
 * <p>Almost every gated field reads {@code orFilters} ({@code [AndFilterInput!]}), which can express
 * the OR-of-ANDs ownership predicate (own-it OR nobody-owns-it). The one exception is the legacy
 * {@code browse} resolver, which only reads the flat {@code filters} ({@code [FacetFilterInput!]})
 * list — a pure AND. There we can only inject the "own-it" criterion; the "nobody-owns-it" branch
 * cannot be expressed, so legacy browse is slightly over-restrictive (unowned assets are hidden).
 * The modern {@code browseV2} path is unaffected.
 */
public class FieldArgumentMutators {

    /** Fields whose resolver consumes {@code orFilters} — full OR-of-ANDs ownership predicate. */
    private static final Set<String> OR_FILTER_FIELDS = Set.of(
        "searchAcrossEntities",
        "scrollAcrossEntities",
        "searchAcrossLineage",
        "scrollAcrossLineage",
        "browseV2",
        "aggregateAcrossEntities",
        "autoComplete",
        "autoCompleteForMultiple",
        "search"
    );

    /** Fields whose resolver only consumes the flat {@code filters} list (pure AND). */
    private static final Set<String> FLAT_FILTER_FIELDS = Set.of(
        "browse"
    );

    public boolean isOwnershipGated(@Nonnull String fieldName) {
        return OR_FILTER_FIELDS.contains(fieldName) || FLAT_FILTER_FIELDS.contains(fieldName);
    }

    /**
     * Mutates {@code input} in place, injecting the ownership predicate into the appropriate filter
     * slot for the given field.
     *
     * @param ownershipFilter the OR-of-ANDs ownership predicate from {@link
     *     io.datahub.ownership.filter.OwnershipFilterBuilder} — one {@code AndFilterInput} per
     *     disjunct (own-it, nobody-owns-it).
     */
    public void applyOwnershipFilter(
            @Nonnull String fieldName,
            @Nonnull Map<String, Object> input,
            @Nonnull List<Map<String, Object>> ownershipFilter) {

        if (OR_FILTER_FIELDS.contains(fieldName)) {
            applyToOrFilters(input, ownershipFilter);
        } else if (FLAT_FILTER_FIELDS.contains(fieldName)) {
            applyToFlatFilters(input, ownershipFilter);
        }
    }

    /**
     * Distributes the ownership predicate across the existing {@code orFilters} via the algebraic
     * identity {@code (E1 OR E2) AND (O1 OR O2) = (E1 AND O1) OR (E1 AND O2) OR (E2 AND O1) OR
     * (E2 AND O2)}. With no existing filters the predicate becomes the {@code orFilters} directly.
     */
    @SuppressWarnings("unchecked")
    private void applyToOrFilters(Map<String, Object> input, List<Map<String, Object>> ownershipFilter) {
        List<Map<String, Object>> existing = (List<Map<String, Object>>) input.get("orFilters");
        if (existing == null || existing.isEmpty()) {
            input.put("orFilters", deepCopyDisjuncts(ownershipFilter));
            return;
        }
        List<Map<String, Object>> merged = new ArrayList<>(existing.size() * ownershipFilter.size());
        for (Map<String, Object> existingDisjunct : existing) {
            List<Map<String, Object>> existingAnds =
                (List<Map<String, Object>>) existingDisjunct.getOrDefault("and", List.of());
            for (Map<String, Object> ownershipDisjunct : ownershipFilter) {
                List<Map<String, Object>> ownershipAnds =
                    (List<Map<String, Object>>) ownershipDisjunct.getOrDefault("and", List.of());
                List<Map<String, Object>> newAnd =
                    new ArrayList<>(existingAnds.size() + ownershipAnds.size());
                newAnd.addAll(existingAnds);
                newAnd.addAll(ownershipAnds);
                Map<String, Object> newDisjunct = new LinkedHashMap<>(existingDisjunct);
                newDisjunct.put("and", newAnd);
                merged.add(newDisjunct);
            }
        }
        input.put("orFilters", merged);
    }

    /**
     * Flat {@code filters} is a pure AND, so only the first ownership disjunct (the own-it
     * criterion) can be injected — the no-owners branch is dropped here. See class javadoc.
     */
    @SuppressWarnings("unchecked")
    private void applyToFlatFilters(Map<String, Object> input, List<Map<String, Object>> ownershipFilter) {
        List<Map<String, Object>> ownerMatchAnds =
            (List<Map<String, Object>>) ownershipFilter.get(0).getOrDefault("and", List.of());
        List<Map<String, Object>> existing = (List<Map<String, Object>>) input.get("filters");
        List<Map<String, Object>> merged = new ArrayList<>(
            (existing == null ? 0 : existing.size()) + ownerMatchAnds.size());
        if (existing != null) merged.addAll(existing);
        merged.addAll(ownerMatchAnds);
        input.put("filters", merged);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> deepCopyDisjuncts(List<Map<String, Object>> disjuncts) {
        List<Map<String, Object>> copy = new ArrayList<>(disjuncts.size());
        for (Map<String, Object> disjunct : disjuncts) {
            Map<String, Object> d = new LinkedHashMap<>(disjunct);
            Object and = disjunct.get("and");
            if (and instanceof List) {
                d.put("and", new ArrayList<>((List<Map<String, Object>>) and));
            }
            copy.add(d);
        }
        return copy;
    }
}
