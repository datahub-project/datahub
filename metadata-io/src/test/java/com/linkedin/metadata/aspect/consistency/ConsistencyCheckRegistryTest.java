package com.linkedin.metadata.aspect.consistency;

import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for ConsistencyCheckRegistry including on-demand check filtering. */
public class ConsistencyCheckRegistryTest {

  private ConsistencyCheckRegistry registry;

  // Test check that is NOT on-demand (default behavior)
  private static class DefaultCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "default";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Default Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "A default (non on-demand) check";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "testEntity";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("testAspect"));
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull java.util.Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  // Test check that IS on-demand only
  private static class OnDemandOnlyCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "on-demand-only";
    }

    @Override
    @Nonnull
    public String getName() {
      return "On-Demand Only Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "An on-demand only check";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "testEntity";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("testAspect"));
    }

    @Override
    public boolean isOnDemandOnly() {
      return true;
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull java.util.Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  // Another default check for a different entity type
  private static class OtherEntityDefaultCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "other-entity-default";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Other Entity Default Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "A default check for another entity type";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "otherEntity";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("otherAspect"));
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull java.util.Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  // On-demand check for a different entity type that is the ONLY check for that type
  private static class OnlyOnDemandEntityCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "only-on-demand-entity";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Only On-Demand Entity Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "The only check for this entity type, and it's on-demand";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "onDemandOnlyEntity";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("someAspect"));
    }

    @Override
    public boolean isOnDemandOnly() {
      return true;
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull java.util.Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  // Wildcard check that applies to all entity types (on-demand only)
  private static class WildcardCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "wildcard-check";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Wildcard Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "A check that applies to all entity types";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE;
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of());
    }

    @Override
    public boolean isOnDemandOnly() {
      return true; // Wildcard checks are typically on-demand only
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull java.util.Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  // Wildcard check that is NOT on-demand (default wildcard)
  private static class DefaultWildcardCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "default-wildcard-check";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Default Wildcard Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "A default check that applies to all entity types";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE;
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of());
    }

    @Override
    public boolean isOnDemandOnly() {
      return false; // This is a default wildcard check
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull java.util.Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  // Duplicate check to test duplicate ID handling
  private static class DuplicateDefaultCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "default"; // Same ID as DefaultCheck
    }

    @Override
    @Nonnull
    public String getName() {
      return "Duplicate Default Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "A duplicate check with the same ID";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "testEntity";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("duplicateAspect"));
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull java.util.Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  @BeforeMethod
  public void setup() {
    registry =
        new ConsistencyCheckRegistry(
            List.of(
                new DefaultCheck(),
                new OnDemandOnlyCheck(),
                new OtherEntityDefaultCheck(),
                new OnlyOnDemandEntityCheck()));
  }

  @Test
  public void testGetAllReturnsAllChecks() {
    assertEquals(registry.getAll().size(), 4);
  }

  @Test
  public void testGetDefaultChecksExcludesOnDemand() {
    List<ConsistencyCheck> defaultChecks = registry.getDefaultChecks();

    assertEquals(defaultChecks.size(), 2);
    assertTrue(defaultChecks.stream().noneMatch(ConsistencyCheck::isOnDemandOnly));
    assertTrue(defaultChecks.stream().anyMatch(c -> c.getId().equals("default")));
    assertTrue(defaultChecks.stream().anyMatch(c -> c.getId().equals("other-entity-default")));
  }

  @Test
  public void testGetEntityTypesReturnsAll() {
    Set<String> entityTypes = registry.getEntityTypes();

    assertEquals(entityTypes.size(), 3);
    assertTrue(entityTypes.contains("testEntity"));
    assertTrue(entityTypes.contains("otherEntity"));
    assertTrue(entityTypes.contains("onDemandOnlyEntity"));
  }

  @Test
  public void testGetDefaultEntityTypesExcludesOnDemandOnlyEntities() {
    Set<String> defaultEntityTypes = registry.getDefaultEntityTypes();

    // onDemandOnlyEntity should be excluded since its only check is on-demand
    assertEquals(defaultEntityTypes.size(), 2);
    assertTrue(defaultEntityTypes.contains("testEntity"));
    assertTrue(defaultEntityTypes.contains("otherEntity"));
    assertFalse(defaultEntityTypes.contains("onDemandOnlyEntity"));
  }

  @Test
  public void testGetByEntityTypeReturnsAll() {
    List<ConsistencyCheck> testEntityChecks = registry.getByEntityType("testEntity");

    assertEquals(testEntityChecks.size(), 2);
  }

  @Test
  public void testGetDefaultByEntityTypeExcludesOnDemand() {
    List<ConsistencyCheck> defaultTestEntityChecks = registry.getDefaultByEntityType("testEntity");

    assertEquals(defaultTestEntityChecks.size(), 1);
    assertEquals(defaultTestEntityChecks.get(0).getId(), "default");
    assertFalse(defaultTestEntityChecks.get(0).isOnDemandOnly());
  }

  @Test
  public void testGetDefaultByEntityTypeAndIdsWithNullReturnsDefaults() {
    List<ConsistencyCheck> checks = registry.getDefaultByEntityTypeAndIds("testEntity", null);

    assertEquals(checks.size(), 1);
    assertEquals(checks.get(0).getId(), "default");
  }

  @Test
  public void testGetDefaultByEntityTypeAndIdsWithEmptyReturnsDefaults() {
    List<ConsistencyCheck> checks = registry.getDefaultByEntityTypeAndIds("testEntity", List.of());

    assertEquals(checks.size(), 1);
    assertEquals(checks.get(0).getId(), "default");
  }

  @Test
  public void testGetDefaultByEntityTypeAndIdsWithExplicitIncludesOnDemand() {
    // When checkIds is explicitly specified, on-demand checks CAN be included
    List<ConsistencyCheck> checks =
        registry.getDefaultByEntityTypeAndIds("testEntity", List.of("on-demand-only"));

    assertEquals(checks.size(), 1);
    assertEquals(checks.get(0).getId(), "on-demand-only");
    assertTrue(checks.get(0).isOnDemandOnly());
  }

  @Test
  public void testGetDefaultByEntityTypeAndIdsWithMixedIds() {
    // Can mix default and on-demand checks when explicitly specified
    List<ConsistencyCheck> checks =
        registry.getDefaultByEntityTypeAndIds("testEntity", List.of("default", "on-demand-only"));

    assertEquals(checks.size(), 2);
  }

  @Test
  public void testGetByIdsReturnsAnyCheck() {
    // getByIds returns any check regardless of on-demand status
    List<ConsistencyCheck> checks = registry.getByIds(List.of("on-demand-only", "default"));

    assertEquals(checks.size(), 2);
  }

  @Test
  public void testDerivedCheckId() {
    // Verify that check IDs are correctly derived from class names
    assertEquals(registry.getById("default").map(ConsistencyCheck::getId).orElse(null), "default");
    assertEquals(
        registry.getById("on-demand-only").map(ConsistencyCheck::getId).orElse(null),
        "on-demand-only");
    assertEquals(
        registry.getById("other-entity-default").map(ConsistencyCheck::getId).orElse(null),
        "other-entity-default");
    assertEquals(
        registry.getById("only-on-demand-entity").map(ConsistencyCheck::getId).orElse(null),
        "only-on-demand-entity");
  }

  // ============================================================================
  // Tests for getCheckIds()
  // ============================================================================

  @Test
  public void testGetCheckIdsReturnsAllIds() {
    Set<String> checkIds = registry.getCheckIds();

    assertEquals(checkIds.size(), 4);
    assertTrue(checkIds.contains("default"));
    assertTrue(checkIds.contains("on-demand-only"));
    assertTrue(checkIds.contains("other-entity-default"));
    assertTrue(checkIds.contains("only-on-demand-entity"));
  }

  @Test
  public void testGetCheckIdsIsImmutable() {
    Set<String> checkIds = registry.getCheckIds();

    // Should not be able to modify the returned set
    try {
      checkIds.add("new-check");
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  // ============================================================================
  // Tests for wildcard entity type handling
  // ============================================================================

  @Test
  public void testWildcardCheckRegistration() {
    ConsistencyCheckRegistry registryWithWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new WildcardCheck()));

    // Wildcard check should be registered
    assertTrue(registryWithWildcard.getById("wildcard-check").isPresent());
    assertEquals(registryWithWildcard.size(), 2);
  }

  @Test
  public void testWildcardEntityTypeInEntityTypes() {
    ConsistencyCheckRegistry registryWithWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new WildcardCheck()));

    Set<String> entityTypes = registryWithWildcard.getEntityTypes();

    assertTrue(entityTypes.contains(ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE));
    assertTrue(entityTypes.contains("testEntity"));
  }

  @Test
  public void testGetByEntityTypeAndIdsIncludesWildcardChecks() {
    // When requesting checks by ID for a specific entity type,
    // wildcard checks matching those IDs should be included
    ConsistencyCheckRegistry registryWithWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new WildcardCheck()));

    List<ConsistencyCheck> checks =
        registryWithWildcard.getByEntityTypeAndIds("testEntity", List.of("wildcard-check"));

    assertEquals(checks.size(), 1);
    assertEquals(checks.get(0).getId(), "wildcard-check");
  }

  @Test
  public void testGetByEntityTypeAndIdsMixesTypeAndWildcardChecks() {
    ConsistencyCheckRegistry registryWithWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new WildcardCheck()));

    List<ConsistencyCheck> checks =
        registryWithWildcard.getByEntityTypeAndIds(
            "testEntity", List.of("default", "wildcard-check"));

    assertEquals(checks.size(), 2);
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("default")));
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("wildcard-check")));
  }

  @Test
  public void testGetByEntityTypeIncludesWildcard() {
    ConsistencyCheckRegistry registryWithWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new WildcardCheck()));

    // getByEntityType should include wildcard checks
    List<ConsistencyCheck> checks = registryWithWildcard.getByEntityType("testEntity");

    assertEquals(checks.size(), 2);
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("default")));
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("wildcard-check")));
  }

  @Test
  public void testGetByEntityTypeAndIdsWithNullIncludesWildcard() {
    ConsistencyCheckRegistry registryWithWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new WildcardCheck()));

    // When no checkIds specified, should return type-specific checks AND wildcard checks
    List<ConsistencyCheck> checks = registryWithWildcard.getByEntityTypeAndIds("testEntity", null);

    assertEquals(checks.size(), 2);
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("default")));
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("wildcard-check")));
  }

  @Test
  public void testGetByEntityTypeExactExcludesWildcard() {
    ConsistencyCheckRegistry registryWithWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new WildcardCheck()));

    // getByEntityTypeExact should NOT include wildcard checks
    List<ConsistencyCheck> checks = registryWithWildcard.getByEntityTypeExact("testEntity");

    assertEquals(checks.size(), 1);
    assertEquals(checks.get(0).getId(), "default");
  }

  @Test
  public void testDefaultEntityTypesExcludesWildcard() {
    ConsistencyCheckRegistry registryWithWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new WildcardCheck()));

    Set<String> defaultEntityTypes = registryWithWildcard.getDefaultEntityTypes();

    // Wildcard should be excluded from default entity types
    assertFalse(defaultEntityTypes.contains(ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE));
    assertTrue(defaultEntityTypes.contains("testEntity"));
  }

  @Test
  public void testGetDefaultByEntityTypeIncludesDefaultWildcards() {
    // Registry with a default wildcard check (not on-demand)
    ConsistencyCheckRegistry registryWithDefaultWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new DefaultWildcardCheck()));

    List<ConsistencyCheck> checks =
        registryWithDefaultWildcard.getDefaultByEntityType("testEntity");

    // Should include both the entity-specific default and the wildcard default
    assertEquals(checks.size(), 2);
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("default")));
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("default-wildcard-check")));
  }

  @Test
  public void testGetDefaultByEntityTypeExcludesOnDemandWildcards() {
    // Registry with an on-demand wildcard check
    ConsistencyCheckRegistry registryWithOnDemandWildcard =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new WildcardCheck()));

    List<ConsistencyCheck> checks =
        registryWithOnDemandWildcard.getDefaultByEntityType("testEntity");

    // Should only include the entity-specific default, not the on-demand wildcard
    assertEquals(checks.size(), 1);
    assertEquals(checks.get(0).getId(), "default");
    assertFalse(checks.stream().anyMatch(c -> c.getId().equals("wildcard-check")));
  }

  @Test
  public void testGetByEntityTypeIncludesBothDefaultAndOnDemandWildcards() {
    // Registry with both on-demand and default wildcard checks
    ConsistencyCheckRegistry registryWithBothWildcards =
        new ConsistencyCheckRegistry(
            List.of(new DefaultCheck(), new WildcardCheck(), new DefaultWildcardCheck()));

    List<ConsistencyCheck> checks = registryWithBothWildcards.getByEntityType("testEntity");

    // Should include the entity-specific check and both wildcard checks
    assertEquals(checks.size(), 3);
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("default")));
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("wildcard-check")));
    assertTrue(checks.stream().anyMatch(c -> c.getId().equals("default-wildcard-check")));
  }

  @Test
  public void testGetByEntityTypeForWildcardReturnsOnlyWildcards() {
    // When querying for the wildcard entity type itself, should only return wildcard checks
    ConsistencyCheckRegistry registryWithWildcard =
        new ConsistencyCheckRegistry(
            List.of(new DefaultCheck(), new WildcardCheck(), new DefaultWildcardCheck()));

    List<ConsistencyCheck> checks =
        registryWithWildcard.getByEntityType(ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE);

    assertEquals(checks.size(), 2);
    assertTrue(checks.stream().allMatch(c -> c.getEntityType().equals("*")));
  }

  // ============================================================================
  // Tests for duplicate check ID handling
  // ============================================================================

  @Test
  public void testDuplicateCheckIdOverwritesPrevious() {
    // When registering checks with duplicate IDs, the later one should overwrite
    ConsistencyCheckRegistry registryWithDuplicate =
        new ConsistencyCheckRegistry(List.of(new DefaultCheck(), new DuplicateDefaultCheck()));

    // Should only have 1 check with ID "default"
    assertEquals(registryWithDuplicate.size(), 1);

    // The later registration should win
    ConsistencyCheck check = registryWithDuplicate.getById("default").orElse(null);
    assertNotNull(check);
    assertEquals(check.getName(), "Duplicate Default Check");
    assertTrue(check.getRequiredAspects().isPresent());
    assertTrue(check.getRequiredAspects().get().contains("duplicateAspect"));
  }

  @Test
  public void testDuplicateCheckIdPreservesOtherChecks() {
    // Other checks should still be registered
    ConsistencyCheckRegistry registryWithDuplicate =
        new ConsistencyCheckRegistry(
            List.of(
                new DefaultCheck(), new OtherEntityDefaultCheck(), new DuplicateDefaultCheck()));

    assertEquals(registryWithDuplicate.size(), 2);
    assertTrue(registryWithDuplicate.getById("default").isPresent());
    assertTrue(registryWithDuplicate.getById("other-entity-default").isPresent());
  }
}
