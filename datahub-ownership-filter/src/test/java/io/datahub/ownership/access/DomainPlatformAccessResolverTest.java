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

    private static EntitySpec spec(String name, boolean domainAssignable) {
        EntitySpec s = mock(EntitySpec.class);
        when(s.getName()).thenReturn(name);
        when(s.hasAspect("domains")).thenReturn(domainAssignable);
        return s;
    }

    /**
     * The accessible set must come from a filtered aggregation scoped to the entity types that can
     * be assigned to a domain (the {@code domains} aspect, read live from the registry) — never a
     * hardcoded entity list and never a null/all-index scope. The domain/platform entities
     * themselves must be excluded (they lack the domains aspect) or their self-referential
     * domains/platform fields leak every value back in.
     */
    @Test
    @SuppressWarnings("unchecked")
    void aggregatesOverDomainAssignableEntitiesReadFromRegistry() {
        EntitySearchService ess = mock(EntitySearchService.class);
        OperationContext opCtx = mock(OperationContext.class);
        EntityRegistry registry = mock(EntityRegistry.class);
        when(opCtx.getEntityRegistry()).thenReturn(registry);

        Map<String, EntitySpec> specs = new HashMap<>();
        specs.put("dataset", spec("dataset", true));
        specs.put("dashboard", spec("dashboard", true));
        specs.put("tag", spec("tag", false));
        specs.put("domain", spec("domain", false));
        when(registry.getEntitySpecs()).thenReturn(specs);

        when(ess.aggregateByValue(any(), any(), eq("domains"), any(), any()))
                .thenReturn(Map.of("urn:li:domain:CBP", 3L));
        when(ess.aggregateByValue(any(), any(), eq("platform"), any(), any()))
                .thenReturn(Map.of("urn:li:dataPlatform:mysql", 3L));

        DomainPlatformAccessResolver.AccessSets sets =
                new DomainPlatformAccessResolver(ess)
                        .resolve(opCtx, urn("urn:li:corpuser:alice"), List.of(urn("urn:li:corpGroup:g1")));

        ArgumentCaptor<List<String>> entityCap = ArgumentCaptor.forClass(List.class);
        verify(ess, atLeastOnce())
                .aggregateByValue(any(), entityCap.capture(), eq("domains"), any(Filter.class), any());

        List<String> entityNames = entityCap.getValue();
        assertThat(entityNames).contains("dataset", "dashboard");
        assertThat(entityNames).doesNotContain("tag", "domain");

        assertThat(sets.domains()).containsExactly("urn:li:domain:CBP");
        assertThat(sets.platforms()).containsExactly("urn:li:dataPlatform:mysql");
    }
}
