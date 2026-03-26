package com.linkedin.metadata.aspect.consistency.check;

import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Helper class for running aggregate consistency check tests.
 *
 * <p>This helper uses a {@link ConsistencyCheckRegistry} to run all registered checks for a given
 * entity type and provides assertion methods for validating aggregate results.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * // Setup
 * List<ConsistencyCheck> checks = List.of(
 *     new CheckA(registry),
 *     new CheckB(registry)
 * );
 * ConsistencyCheckRegistry registry = new ConsistencyCheckRegistry(checks);
 * AggregateCheckTestHelper helper = new AggregateCheckTestHelper(registry);
 *
 * // Run checks
 * List<Issue> issues = helper.runChecksForEntityType("assertion", context, entities);
 *
 * // Assert results
 * helper.assertTotalIssueCount(issues, 1);
 * helper.assertIssueFromCheck(issues, "check-a-id", urn, FixType.UPSERT);
 * }</pre>
 */
public class AggregateCheckTestHelper {

  private final ConsistencyCheckRegistry registry;

  /**
   * Create a new helper with the given registry.
   *
   * @param registry the consistency check registry containing all checks to run
   */
  public AggregateCheckTestHelper(@Nonnull ConsistencyCheckRegistry registry) {
    this.registry = registry;
  }

  /**
   * Run all checks for an entity type and return aggregated issues.
   *
   * <p>Iterates through all registered checks for the entity type and calls each check's {@link
   * ConsistencyCheck#check} method, aggregating all returned issues.
   *
   * @param entityType entity type to check (e.g., "assertion", "monitor")
   * @param context check context with operation context, entity service, etc.
   * @param entityResponses map of URN to entity response
   * @return list of all issues found across all checks
   */
  public List<ConsistencyIssue> runChecksForEntityType(
      @Nonnull String entityType,
      @Nonnull CheckContext context,
      @Nonnull Map<Urn, EntityResponse> entityResponses) {

    List<ConsistencyCheck> checks = registry.getByEntityType(entityType);
    List<ConsistencyIssue> allIssues = new ArrayList<>();

    for (ConsistencyCheck check : checks) {
      List<ConsistencyIssue> issues = check.check(context, entityResponses);
      allIssues.addAll(issues);
    }

    return allIssues;
  }

  /**
   * Assert that an issue was created by a specific check for a specific URN with the expected fix
   * type.
   *
   * @param issues list of issues to search
   * @param checkId expected check ID
   * @param urn expected entity URN
   * @param fixType expected fix type
   */
  public void assertIssueFromCheck(
      @Nonnull List<ConsistencyIssue> issues,
      @Nonnull String checkId,
      @Nonnull Urn urn,
      @Nonnull ConsistencyFixType fixType) {
    ConsistencyIssue found =
        issues.stream()
            .filter(
                i ->
                    checkId.equals(i.getCheckId())
                        && urn.equals(i.getEntityUrn())
                        && fixType.equals(i.getFixType()))
            .findFirst()
            .orElse(null);

    assertNotNull(
        found,
        String.format(
            "Expected issue from check '%s' for URN '%s' with fix type '%s' but not found. "
                + "Found issues: %s",
            checkId, urn, fixType, formatIssues(issues)));
  }

  /**
   * Assert that an issue was created by a specific check for a specific URN (any fix type).
   *
   * @param issues list of issues to search
   * @param checkId expected check ID
   * @param urn expected entity URN
   */
  public void assertIssueFromCheck(
      @Nonnull List<ConsistencyIssue> issues, @Nonnull String checkId, @Nonnull Urn urn) {
    ConsistencyIssue found =
        issues.stream()
            .filter(i -> checkId.equals(i.getCheckId()) && urn.equals(i.getEntityUrn()))
            .findFirst()
            .orElse(null);

    assertNotNull(
        found,
        String.format(
            "Expected issue from check '%s' for URN '%s' but not found. Found issues: %s",
            checkId, urn, formatIssues(issues)));
  }

  /**
   * Assert that no issue was created by a specific check.
   *
   * @param issues list of issues to search
   * @param checkId check ID that should not have created issues
   */
  public void assertNoIssueFromCheck(
      @Nonnull List<ConsistencyIssue> issues, @Nonnull String checkId) {
    List<ConsistencyIssue> found =
        issues.stream().filter(i -> checkId.equals(i.getCheckId())).collect(Collectors.toList());

    assertTrue(
        found.isEmpty(),
        String.format("Expected no issues from check '%s' but found: %s", checkId, found));
  }

  /**
   * Assert the total number of issues.
   *
   * @param issues list of issues
   * @param expected expected count
   */
  public void assertTotalIssueCount(@Nonnull List<ConsistencyIssue> issues, int expected) {
    assertEquals(
        issues.size(),
        expected,
        String.format(
            "Expected %d issues but found %d: %s", expected, issues.size(), formatIssues(issues)));
  }

  /**
   * Assert that only specific checks fired (created issues).
   *
   * @param issues list of issues
   * @param expectedCheckIds set of check IDs that should have fired
   */
  public void assertOnlyTheseChecksFired(
      @Nonnull List<ConsistencyIssue> issues, @Nonnull Set<String> expectedCheckIds) {
    Set<String> actualCheckIds = getCheckIdsThatFired(issues);

    assertEquals(
        actualCheckIds,
        expectedCheckIds,
        String.format(
            "Expected only checks %s to fire, but found %s", expectedCheckIds, actualCheckIds));
  }

  /**
   * Get the set of check IDs that created issues.
   *
   * @param issues list of issues
   * @return set of check IDs that fired
   */
  public Set<String> getCheckIdsThatFired(@Nonnull List<ConsistencyIssue> issues) {
    return issues.stream().map(ConsistencyIssue::getCheckId).collect(Collectors.toSet());
  }

  /**
   * Get all issues from a specific check.
   *
   * @param issues list of issues
   * @param checkId check ID to filter by
   * @return list of issues from that check
   */
  public List<ConsistencyIssue> getIssuesFromCheck(
      @Nonnull List<ConsistencyIssue> issues, @Nonnull String checkId) {
    return issues.stream().filter(i -> checkId.equals(i.getCheckId())).collect(Collectors.toList());
  }

  /**
   * Get issue from a specific check for a specific URN (assumes at most one).
   *
   * @param issues list of issues
   * @param checkId check ID to filter by
   * @param urn entity URN to filter by
   * @return the issue, or null if not found
   */
  @Nullable
  public ConsistencyIssue getIssue(
      @Nonnull List<ConsistencyIssue> issues, @Nonnull String checkId, @Nonnull Urn urn) {
    return issues.stream()
        .filter(i -> checkId.equals(i.getCheckId()) && urn.equals(i.getEntityUrn()))
        .findFirst()
        .orElse(null);
  }

  /** Format issues for error messages. */
  private String formatIssues(List<ConsistencyIssue> issues) {
    if (issues.isEmpty()) {
      return "[]";
    }
    return issues.stream()
        .map(
            i ->
                String.format(
                    "{checkId=%s, urn=%s, fixType=%s}",
                    i.getCheckId(), i.getEntityUrn(), i.getFixType()))
        .collect(Collectors.joining(", ", "[", "]"));
  }
}
