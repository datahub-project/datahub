package io.datahubproject.wiring;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** The ratchet's decision logic, separate from Gradle and file I/O so it can be unit tested. */
final class WiringCoverageEvaluator {

  private WiringCoverageEvaluator() {}

  record Result(
      Set<String> covered,
      Set<String> uncovered,
      Map<String, Set<String>> uncalledBeanMethods,
      Set<String> newUncovered,
      Set<String> stale) {}

  /**
   * @param targets configuration class to its own {@code @Bean} method names
   * @param recorded lines written by the recorder ({@code C <class>}, {@code M <class>#<method>})
   * @param baseline class names listed in {@code wiring-baseline.txt}
   */
  static Result evaluate(
      Map<String, Set<String>> targets, Set<String> recorded, Set<String> baseline) {
    Set<String> covered = new TreeSet<>();
    Set<String> uncovered = new TreeSet<>();
    Map<String, Set<String>> uncalledBeanMethods = new TreeMap<>();
    for (Map.Entry<String, Set<String>> e : targets.entrySet()) {
      String cls = e.getKey();
      Set<String> beanMethods = e.getValue();
      boolean isCovered;
      if (beanMethods.isEmpty()) {
        isCovered = recorded.contains("C " + cls);
      } else {
        // Only the class's own @Bean methods count: an inherited one ran for the parent's wiring.
        Set<String> uncalled =
            beanMethods.stream()
                .filter(m -> !recorded.contains("M " + cls + "#" + m))
                .collect(Collectors.toCollection(TreeSet::new));
        isCovered = uncalled.size() < beanMethods.size();
        if (isCovered && !uncalled.isEmpty()) {
          uncalledBeanMethods.put(cls, uncalled);
        }
      }
      (isCovered ? covered : uncovered).add(cls);
    }
    Set<String> newUncovered = new TreeSet<>(uncovered);
    newUncovered.removeAll(baseline);
    Set<String> stale = new TreeSet<>(baseline);
    stale.removeAll(uncovered);
    return new Result(covered, uncovered, uncalledBeanMethods, newUncovered, stale);
  }

  /** Class names in a baseline file, ignoring blank lines and {@code #} comments. */
  static Set<String> parseBaseline(List<String> lines) {
    Set<String> baseline = new TreeSet<>();
    for (String line : lines) {
      String t = line.trim();
      if (!t.isEmpty() && !t.startsWith("#")) {
        baseline.add(t);
      }
    }
    return baseline;
  }

  /** The baseline without {@code stale} entries; comments and order are kept, nothing is added. */
  static List<String> prune(List<String> lines, Set<String> stale) {
    return lines.stream().filter(l -> !stale.contains(l.trim())).collect(Collectors.toList());
  }
}
