package com.linkedin.metadata.timeseries;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.testng.Assert.assertFalse;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import java.time.Instant;
import java.util.stream.Stream;
import org.testng.annotations.Test;

/**
 * Architectural rule: code that builds a time-windowed Elasticsearch/OpenSearch query must take
 * "now" from the canonical clock on {@code OperationContext}, not from the wall clock directly.
 *
 * <p>Nine call sites were converted to {@code opContext.canonicalNow()}; nothing stops the tenth
 * from reaching for the wall clock again, and the symptom - a query unique per request that can
 * never reuse a cache entry - is invisible in review.
 *
 * <p>A tripwire for the likely mistake, not a proof: it lists the two forms that occur in practice,
 * not every clock API. Covers this module only - {@code datahub-graphql-core} has no ArchUnit
 * dependency, and adding one would mean regenerating lockfiles. Mirrors {@code
 * AspectDaoCorrectnessTest}.
 */
public class QueryTimeCanonicalizationCorrectnessTest {

  /**
   * Packages that only query timeseries/usage indices, so any wall-clock read in them is a query
   * bound. {@code com.linkedin.metadata.client} is excluded on purpose: {@code JavaEntityClient}
   * reads the clock there for audit stamps, which must stay exact.
   */
  private static final String[] QUERY_WINDOW_PACKAGES = {
    "com.linkedin.metadata.timeseries", "com.linkedin.metadata.datahubusage",
  };

  /** The one usage-query class outside those packages, checked by name rather than by package. */
  private static final String USAGE_STATS_CLIENT =
      "com.linkedin.metadata.client.UsageStatsJavaClient";

  /** Same packages in {@code resideInAnyPackage} syntax, where {@code ..} means "and below". */
  private static final String[] QUERY_WINDOW_PACKAGE_MATCHERS =
      Stream.of(QUERY_WINDOW_PACKAGES).map(pkg -> pkg + "..").toArray(String[]::new);

  @Test
  public void queryWindowsMustComeFromTheCanonicalClock() {
    // Jars are left in: this module's own classes come from metadata-io.jar on the test classpath,
    // so excluding jars imports nothing and the rule passes vacuously. Dependencies share these
    // package names, so a violation may be reported from another module.
    final JavaClasses classes =
        new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages(
                "com.linkedin.metadata.timeseries",
                "com.linkedin.metadata.client",
                "com.linkedin.metadata.datahubusage");
    assertFalse(
        classes.isEmpty(), "imported no classes — the rule would pass without checking anything");

    noClasses()
        .that()
        .resideInAnyPackage(QUERY_WINDOW_PACKAGE_MATCHERS)
        .or()
        .haveFullyQualifiedName(USAGE_STATS_CLIENT)
        .should()
        .callMethod(System.class, "currentTimeMillis")
        .orShould()
        .callMethod(Instant.class, "now")
        .because(
            "a wall-clock window is unique per request and can never reuse a shard request cache "
                + "entry; use opContext.canonicalNow(), or Instant.now(clock) if you need exact "
                + "wall time")
        .check(classes);
  }
}
