package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.QueryCanonicalizationConfiguration;
import com.linkedin.metadata.config.search.TimeCanonicalizationConfiguration;
import com.linkedin.metadata.utils.elasticsearch.canonicalization.QueryTimeCanonicalizer;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.testng.annotations.Test;

/**
 * Covers the seam that makes the canonicalizer reachable without threading it through call sites:
 * it hangs off OperationContext, and derived contexts must inherit it.
 */
public class OperationContextCanonicalizationTest {

  private static QueryTimeCanonicalizer canonicalizer(String fixedNow) {
    return QueryTimeCanonicalizer.fromConfig(
        QueryCanonicalizationConfiguration.builder()
            .enabled(true)
            .time(
                TimeCanonicalizationConfiguration.builder()
                    .enabled(true)
                    .bucketSize("5m")
                    .timezone("UTC")
                    .rounding("EXPAND")
                    .build())
            .build(),
        null,
        Clock.fixed(Instant.parse(fixedNow), ZoneOffset.UTC));
  }

  /**
   * The ~50 existing OperationContext construction sites set no canonicalizer. They must keep
   * working and behave exactly as they did before the feature existed.
   */
  @Test
  public void testUnconfiguredContextIsPassThrough() {
    final OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();

    assertNotNull(opContext.getQueryTimeCanonicalizer());
    assertSame(opContext.getQueryTimeCanonicalizer(), QueryTimeCanonicalizer.DISABLED);

    final QueryTimeCanonicalizer.CanonicalNow now = opContext.canonicalNow();
    assertFalse(now.isCanonicalized());
    assertEquals(now.reference(), now.raw());
    assertEquals(now.upperBound(), now.raw());
  }

  @Test
  public void testCanonicalizerIsAppliedWhenAttached() {
    final OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization()
            .withQueryTimeCanonicalizer(canonicalizer("2026-08-16T19:03:42Z"));

    final QueryTimeCanonicalizer.CanonicalNow now = opContext.canonicalNow();
    assertTrue(now.isCanonicalized());
    assertEquals(now.raw(), Instant.parse("2026-08-16T19:03:42Z").toEpochMilli());
    assertEquals(now.reference(), Instant.parse("2026-08-16T19:00:00Z").toEpochMilli());
    assertEquals(now.upperBound(), Instant.parse("2026-08-16T19:05:00Z").toEpochMilli());
  }

  /**
   * The whole design rests on derived contexts inheriting the canonicalizer - session contexts and
   * flag-adjusted contexts are what actually reach the query paths. Lombok's toBuilder() carries
   * it, but that is exactly the kind of thing that breaks silently, so pin it.
   */
  @Test
  public void testDerivedContextsInheritCanonicalizer() {
    final QueryTimeCanonicalizer expected = canonicalizer("2026-08-16T19:03:42Z");
    final OperationContext base =
        TestOperationContexts.systemContextNoSearchAuthorization()
            .withQueryTimeCanonicalizer(expected);

    assertSame(
        base.withSearchFlags(flags -> flags.setSkipCache(true)).getQueryTimeCanonicalizer(),
        expected,
        "withSearchFlags must not drop the canonicalizer");
    assertSame(
        base.withLineageFlags(flags -> flags).getQueryTimeCanonicalizer(),
        expected,
        "withLineageFlags must not drop the canonicalizer");
  }
}
