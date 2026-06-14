package io.datahub.ownership.filter;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
class OwnershipFilterBuilderTest {

    private final String alice = "urn:li:corpuser:alice";
    private final String g1 = "urn:li:corpGroup:g1";
    private final String g2 = "urn:li:corpGroup:g2";

    @Test
    void buildsTwoDisjuncts_ownerMatchAndNoOwners() {
        List<Map<String, Object>> result = new OwnershipFilterBuilder()
            .injectOwnershipFilter(alice, List.of(g1, g2));

        // Disjunct 1: owner match. Disjunct 2: no-owners (visible to all).
        assertThat(result).hasSize(2);

        Map<String, Object> ownerMatch = ((List<Map<String, Object>>) result.get(0).get("and")).get(0);
        assertThat(ownerMatch.get("field")).isEqualTo("owners");
        assertThat(ownerMatch.get("condition")).isEqualTo("EQUAL");
        assertThat((List<String>) ownerMatch.get("values")).containsExactlyInAnyOrder(alice, g1, g2);

        Map<String, Object> noOwners = ((List<Map<String, Object>>) result.get(1).get("and")).get(0);
        assertThat(noOwners.get("field")).isEqualTo("owners");
        assertThat(noOwners.get("condition")).isEqualTo("EXISTS");
        assertThat(noOwners.get("negated")).isEqualTo(true);
    }

    @Test
    void ownerMatchIncludesOnlyActorWhenNoGroups() {
        List<Map<String, Object>> result = new OwnershipFilterBuilder()
            .injectOwnershipFilter(alice, List.of());

        Map<String, Object> ownerMatch = ((List<Map<String, Object>>) result.get(0).get("and")).get(0);
        assertThat((List<String>) ownerMatch.get("values")).containsExactly(alice);

        // The no-owners disjunct is always present.
        Map<String, Object> noOwners = ((List<Map<String, Object>>) result.get(1).get("and")).get(0);
        assertThat(noOwners.get("condition")).isEqualTo("EXISTS");
        assertThat(noOwners.get("negated")).isEqualTo(true);
    }
}
