package io.datahubproject.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class WiringCoverageEvaluatorTest {

  private static final Map<String, Set<String>> TARGETS =
      Map.of(
          "a.FactoryWithBeans", Set.of("one", "two"),
          "a.PlainConfig", Set.of(),
          "a.Untested", Set.of("three"));

  @Test
  void coverageNeedsOneOwnBeanMethodOrInstantiation() {
    var result =
        WiringCoverageEvaluator.evaluate(
            TARGETS,
            Set.of("M a.FactoryWithBeans#one", "C a.PlainConfig", "C a.Untested"),
            Set.of("a.Untested"));

    assertEquals(Set.of("a.FactoryWithBeans", "a.PlainConfig"), result.covered());
    assertEquals(Map.of("a.FactoryWithBeans", Set.of("two")), result.uncalledBeanMethods());
    assertEquals(Set.of(), result.newUncovered());
    assertEquals(Set.of(), result.stale());
  }

  @Test
  void inheritedBeanMethodDoesNotCoverTheSubclass() {
    var result =
        WiringCoverageEvaluator.evaluate(
            TARGETS, Set.of("M a.Untested#inheritedFromParent"), Set.of());

    assertEquals(Set.of("a.FactoryWithBeans", "a.PlainConfig", "a.Untested"), result.newUncovered());
  }

  @Test
  void failsOnNewUncoveredAndOnStaleOrDeletedBaselineEntries() {
    var result =
        WiringCoverageEvaluator.evaluate(
            TARGETS,
            Set.of("M a.FactoryWithBeans#two", "M a.Untested#three"),
            Set.of("a.FactoryWithBeans", "a.Deleted"));

    assertEquals(Set.of("a.PlainConfig"), result.newUncovered());
    assertEquals(Set.of("a.FactoryWithBeans", "a.Deleted"), result.stale());
  }

  @Test
  void pruneOnlyRemovesStaleEntries() {
    List<String> lines = List.of("# header", "a.Stale", "", "a.Kept");

    assertEquals(Set.of("a.Stale", "a.Kept"), WiringCoverageEvaluator.parseBaseline(lines));
    assertEquals(
        List.of("# header", "", "a.Kept"), WiringCoverageEvaluator.prune(lines, Set.of("a.Stale")));
  }
}
