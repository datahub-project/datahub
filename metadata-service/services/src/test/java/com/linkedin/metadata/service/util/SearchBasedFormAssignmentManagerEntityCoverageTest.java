package com.linkedin.metadata.service.util;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/**
 * Registry-derived guard (Category B): {@link SearchBasedFormAssignmentManager#ENTITY_TYPES} must
 * equal the set of entities that declare the {@code forms} aspect in the entity registry, minus a
 * documented exception set.
 *
 * <p>Documented exceptions (intentionally excluded from form-assignment):
 *
 * <ul>
 *   <li>{@code schemaField} — sub-field entity; forms are assigned at the dataset level, not
 *       per-field.
 *   <li>{@code application} — intentionally excluded; applications own other assets and do not
 *       receive form assignments themselves.
 * </ul>
 */
public class SearchBasedFormAssignmentManagerEntityCoverageTest {

  private static final Set<String> INTENTIONAL_EXCLUSIONS =
      Set.of(
          "schemaField", // sub-field entity; forms assigned at dataset level
          "application" // intentionally excluded from form assignment
          );

  @Test
  public void testEntityTypesMatchesFormsAspectInRegistry() {
    final EntityRegistry registry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));

    final Set<String> entitiesWithFormsAspect =
        registry.getEntitySpecs().values().stream()
            .filter(spec -> spec.hasAspect(Constants.FORMS_ASPECT_NAME))
            .map(spec -> spec.getName())
            .filter(name -> !INTENTIONAL_EXCLUSIONS.contains(name))
            .collect(Collectors.toCollection(TreeSet::new));

    final Set<String> configured = new TreeSet<>(SearchBasedFormAssignmentManager.ENTITY_TYPES);

    assertEquals(
        configured,
        entitiesWithFormsAspect,
        "SearchBasedFormAssignmentManager.ENTITY_TYPES must match the entities that declare the "
            + "`forms` aspect in the registry (excluding intentional exceptions: "
            + INTENTIONAL_EXCLUSIONS
            + "). If you added the `forms` aspect to a new entity, add it to ENTITY_TYPES too "
            + "(or add it to INTENTIONAL_EXCLUSIONS with a comment). "
            + "registry-with-forms-minus-exceptions="
            + entitiesWithFormsAspect
            + " configured="
            + configured);
  }
}
