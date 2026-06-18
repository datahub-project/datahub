package com.linkedin.datahub.graphql.resolvers.operation;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.io.InputStream;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/**
 * Registry-derived guard (Category B): a feature allowlist that gates on a single aspect should be
 * derivable from the registry, not hand-maintained in parallel with it.
 *
 * <p>{@code reportOperation} writes the {@code operation} aspect, so {@link
 * ReportOperationResolver#SUPPORTED_ENTITY_TYPES} must equal exactly the set of entities that
 * declare the {@code operation} aspect in the entity registry. Adding the aspect to a new entity
 * (as {@code dataObject} did) without adding it here — or vice versa — fails this test with a named
 * diff, instead of silently rejecting valid operation reports at runtime.
 *
 * <p>This is the proven pattern: {@code TestEngine} likewise derives its supported entities from
 * the registry (entities with the {@code testResults} aspect) rather than re-listing them by hand.
 */
public class ReportOperationResolverEntityCoverageTest {

  @Test
  public void testSupportedEntityTypesMatchesOperationAspectInRegistry() {
    final InputStream inputStream = ClassLoader.getSystemResourceAsStream("entity-registry.yml");
    final EntityRegistry registry = new ConfigEntityRegistry(inputStream);

    final Set<String> entitiesWithOperationAspect =
        registry.getEntitySpecs().values().stream()
            .filter(spec -> spec.hasAspect(Constants.OPERATION_ASPECT_NAME))
            .map(spec -> spec.getName())
            .collect(Collectors.toCollection(TreeSet::new));

    final Set<String> supported = new TreeSet<>(ReportOperationResolver.SUPPORTED_ENTITY_TYPES);

    assertEquals(
        supported,
        entitiesWithOperationAspect,
        "ReportOperationResolver.SUPPORTED_ENTITY_TYPES must match the entities that declare the "
            + "`operation` aspect in the registry. If you added the `operation` aspect to a new "
            + "entity, add it here too (or remove the aspect). registry-with-operation="
            + entitiesWithOperationAspect
            + " resolver="
            + supported);
  }
}
