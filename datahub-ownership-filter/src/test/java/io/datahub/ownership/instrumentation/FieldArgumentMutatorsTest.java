package io.datahub.ownership.instrumentation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
class FieldArgumentMutatorsTest {

    private final FieldArgumentMutators mutators = new FieldArgumentMutators();

    /** The two-disjunct ownership predicate produced by OwnershipFilterBuilder. */
    private static List<Map<String, Object>> ownershipPredicate() {
        Map<String, Object> ownerMatch = Map.of("field", "owners", "condition", "EQUAL",
            "values", List.of("urn:li:corpuser:alice"));
        Map<String, Object> noOwners = Map.of("field", "owners", "condition", "EXISTS",
            "negated", true);
        return List.of(
            Map.of("and", List.of(ownerMatch)),
            Map.of("and", List.of(noOwners)));
    }

    @Test
    void registryCoversExpectedFields() {
        for (String f : List.of("searchAcrossEntities", "scrollAcrossEntities", "searchAcrossLineage",
                "scrollAcrossLineage", "autoComplete", "autoCompleteForMultiple", "browse",
                "browseV2", "aggregateAcrossEntities", "search")) {
            assertThat(mutators.isOwnershipGated(f)).as(f).isTrue();
        }
        assertThat(mutators.isOwnershipGated("dataset")).isFalse();
        assertThat(mutators.isOwnershipGated("me")).isFalse();
    }

    @Test
    void injectsBothDisjunctsWhenNoExistingOrFilters() {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("query", "*");

        mutators.applyOwnershipFilter("searchAcrossEntities", input, ownershipPredicate());

        List<Map<String, Object>> orFilters = (List<Map<String, Object>>) input.get("orFilters");
        assertThat(orFilters).hasSize(2);
        assertThat(((List<Map<String, Object>>) orFilters.get(0).get("and")).get(0).get("condition"))
            .isEqualTo("EQUAL");
        assertThat(((List<Map<String, Object>>) orFilters.get(1).get("and")).get(0).get("condition"))
            .isEqualTo("EXISTS");
    }

    @Test
    void crossProductWithExistingOrFilters() {
        // existing: a single disjunct (platform = snowflake)
        Map<String, Object> input = new LinkedHashMap<>();
        List<Map<String, Object>> existing = new ArrayList<>();
        existing.add(Map.of("and", new ArrayList<>(List.of(
            Map.of("field", "platform", "condition", "EQUAL", "values", List.of("snowflake"))))));
        input.put("orFilters", existing);

        mutators.applyOwnershipFilter("searchAcrossEntities", input, ownershipPredicate());

        // (platform) AND (ownerMatch OR noOwners) => 2 disjuncts, each platform + one ownership crit.
        List<Map<String, Object>> orFilters = (List<Map<String, Object>>) input.get("orFilters");
        assertThat(orFilters).hasSize(2);
        for (Map<String, Object> disjunct : orFilters) {
            List<Map<String, Object>> ands = (List<Map<String, Object>>) disjunct.get("and");
            assertThat(ands).hasSize(2);
            assertThat(ands.get(0).get("field")).isEqualTo("platform");
            assertThat(ands.get(1).get("field")).isEqualTo("owners");
        }
        assertThat(((Map<String, Object>) ((List<?>) orFilters.get(0).get("and")).get(1)).get("condition"))
            .isEqualTo("EQUAL");
        assertThat(((Map<String, Object>) ((List<?>) orFilters.get(1).get("and")).get(1)).get("condition"))
            .isEqualTo("EXISTS");
    }

    @Test
    void flatFieldGetsOnlyOwnerMatch() {
        // legacy browse only supports a flat AND `filters` list, so only the owner-match criterion
        // is injected (the no-owners OR branch cannot be expressed there).
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("query", "abc");

        mutators.applyOwnershipFilter("browse", input, ownershipPredicate());

        List<Map<String, Object>> filters = (List<Map<String, Object>>) input.get("filters");
        assertThat(filters).hasSize(1);
        assertThat(filters.get(0).get("field")).isEqualTo("owners");
        assertThat(filters.get(0).get("condition")).isEqualTo("EQUAL");
        assertThat(input.get("orFilters")).isNull();
    }
}
