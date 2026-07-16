package com.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitSource;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Unit tests for the controller's Part B heavy-resolver gate wiring ({@link
 * GraphQLController#applyHeavyResolverGate}) — the loop over top-level resolvers (supplied from the
 * single query parse, not re-parsed), first-deny short-circuit, and front-gate lease release on a
 * heavy denial.
 */
public class GraphQLControllerTest {

  private static final List<String> ONE_HEAVY = List.of("searchAcrossEntities");
  private static final String ACTOR = "urn:li:corpuser:tester";

  @Test
  public void testFrontGateDenialShortCircuitsHeavyGate() {
    RateLimitEngine engine = mock(RateLimitEngine.class);
    RateLimitDecision frontGate = denied("scoped:actor");

    RateLimitDecision result =
        GraphQLController.applyHeavyResolverGate(engine, frontGate, ONE_HEAVY, false, ACTOR, null);

    // A request already denied at the front gate is returned untouched; the heavy gate never runs.
    assertSame(result, frontGate);
    verify(engine, never()).consumeHeavyResolver(any(), anyBoolean());
    verify(engine, never()).releaseCapacity(any(), anyBoolean());
    verify(engine, never()).refundScopedChain(any(), any());
  }

  @Test
  public void testAllowedWithNoHeavyResolverReturnsFrontGateDecision() {
    RateLimitEngine engine = mock(RateLimitEngine.class);
    when(engine.consumeHeavyResolver(any(), anyBoolean())).thenReturn(null);
    RateLimitDecision frontGate = allowed();

    RateLimitDecision result =
        GraphQLController.applyHeavyResolverGate(engine, frontGate, ONE_HEAVY, false, ACTOR, null);

    assertSame(result, frontGate);
    verify(engine).consumeHeavyResolver("searchAcrossEntities", false);
    verify(engine, never()).releaseCapacity(any(), anyBoolean());
    verify(engine, never()).refundScopedChain(any(), any());
  }

  @Test
  public void testHeavyResolverDenialReleasesCapacityAndRefundsScopedAndReturnsDenial() {
    RateLimitEngine engine = mock(RateLimitEngine.class);
    RateLimitDecision frontGate = allowed();
    RateLimitDecision heavyDenied = denied("scoped:op:searchAcrossEntities");
    when(engine.consumeHeavyResolver("searchAcrossEntities", false)).thenReturn(heavyDenied);

    RateLimitDecision result =
        GraphQLController.applyHeavyResolverGate(engine, frontGate, ONE_HEAVY, false, ACTOR, null);

    assertSame(result, heavyDenied);
    // Rejecting here unwinds the front gate: release the capacity slot directly from the decision
    // AND refund the scoped tokens, so a request that didn't get through doesn't permanently burn
    // the actor's quota.
    verify(engine).releaseCapacity(frontGate, false);
    verify(engine).refundScopedChain(ACTOR, null);
  }

  @Test
  public void testStopsAtFirstDenyingTopLevelFieldAndRefundsChargedResolvers() {
    RateLimitEngine engine = mock(RateLimitEngine.class);
    RateLimitDecision frontGate = allowed();
    when(engine.consumeHeavyResolver("a", false)).thenReturn(null); // charged, allowed
    RateLimitDecision bDenied = denied("scoped:op:b");
    when(engine.consumeHeavyResolver("b", false)).thenReturn(bDenied);

    RateLimitDecision result =
        GraphQLController.applyHeavyResolverGate(
            engine, frontGate, List.of("a", "b"), false, ACTOR, null);

    assertSame(result, bDenied);
    verify(engine).consumeHeavyResolver("a", false);
    verify(engine).consumeHeavyResolver("b", false);
    // Unwind on the "b" denial: release the front-gate capacity, refund the scoped chain, and
    // refund
    // the already-charged resolver "a" — but NOT "b" (it denied, it never took a token).
    verify(engine).releaseCapacity(frontGate, false);
    verify(engine).refundScopedChain(ACTOR, null);
    verify(engine).refundHeavyResolver("a", false);
    verify(engine, never()).refundHeavyResolver("b", false);
  }

  @Test
  public void testHeavyResolverThrowUnwindsFrontGateAndRethrows() {
    // Fail-open disabled: consumeHeavyResolver rethrows. The gate must release the held front-gate
    // capacity slot (else it leaks) and refund the scoped chain, then propagate the exception.
    RateLimitEngine engine = mock(RateLimitEngine.class);
    RateLimitDecision frontGate = allowed();
    RuntimeException boom = new RuntimeException("store down, fail-closed");
    when(engine.consumeHeavyResolver("a", false)).thenThrow(boom);

    RuntimeException thrown =
        expectThrows(
            RuntimeException.class,
            () ->
                GraphQLController.applyHeavyResolverGate(
                    engine, frontGate, List.of("a"), false, ACTOR, null));

    assertSame(thrown, boom);
    verify(engine).releaseCapacity(frontGate, false);
    verify(engine).refundScopedChain(ACTOR, null);
  }

  @Test
  public void testSystemActorFlagIsPropagated() {
    RateLimitEngine engine = mock(RateLimitEngine.class);
    when(engine.consumeHeavyResolver(any(), anyBoolean())).thenReturn(null);

    GraphQLController.applyHeavyResolverGate(engine, allowed(), ONE_HEAVY, true, ACTOR, null);

    verify(engine).consumeHeavyResolver("searchAcrossEntities", true);
  }

  private static RateLimitDecision allowed() {
    return RateLimitDecision.builder().allowed(true).source(RateLimitSource.GRAPHQL_GATE).build();
  }

  private static RateLimitDecision denied(String ruleId) {
    return RateLimitDecision.builder()
        .allowed(false)
        .denyingRuleId(ruleId)
        .source(RateLimitSource.GRAPHQL_GATE)
        .build();
  }
}
