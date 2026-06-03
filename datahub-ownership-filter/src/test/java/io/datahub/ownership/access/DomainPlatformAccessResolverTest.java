package io.datahub.ownership.access;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DomainPlatformAccessResolverTest {

    static Urn urn(String s) {
        try {
            return Urn.createFromString(s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Regression guard: the aggregation must be scoped to concrete asset entity types, never
     * {@code null}. Passing null makes aggregateByValue query every entity index, where the
     * {@code owners EXISTS negated} clause matches docs on indices without an owners field and the
     * accessible domain/platform set becomes wrong (home-page cards unfiltered/blank on v1.5.x).
     */
    @Test
    @SuppressWarnings("unchecked")
    void aggregationScopedToAssetEntitiesNotAllIndices() {
        EntitySearchService ess = mock(EntitySearchService.class);
        OperationContext opCtx = mock(OperationContext.class);
        EntityRegistry registry = mock(EntityRegistry.class);
        when(opCtx.getEntityRegistry()).thenReturn(registry);

        Map<String, EntitySpec> specs = new HashMap<>();
        for (String name : List.of("dataset", "dashboard", "chart", "corpuser", "tag", "domain")) {
            specs.put(name, mock(EntitySpec.class));
        }
        when(registry.getEntitySpecs()).thenReturn(specs);
        when(ess.aggregateByValue(any(), any(), any(), any(), any())).thenReturn(Map.<String, Long>of());

        new DomainPlatformAccessResolver(ess)
                .resolve(opCtx, urn("urn:li:corpuser:alice"), List.of(urn("urn:li:corpGroup:g1")));

        ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
        verify(ess, atLeastOnce())
                .aggregateByValue(any(), captor.capture(), eq("domains"), any(Filter.class), any());

        List<String> entityNames = captor.getValue();
        assertThat(entityNames).isNotNull().isNotEmpty();
        assertThat(entityNames).contains("dataset", "dashboard", "chart");
        // Non-asset indices must be excluded — they are the source of the over-match.
        assertThat(entityNames).doesNotContain("corpuser", "tag", "domain");
    }
}
