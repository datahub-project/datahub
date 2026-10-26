package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.context.Enrichment;
import com.datahub.context.EnrichmentBundle;
import com.datahub.plugins.auth.authorization.Authorizer;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Optional;
import org.springframework.mock.web.MockHttpServletRequest;
import org.testng.annotations.Test;

/**
 * End-to-end: enrichments stamped on an HttpServletRequest attribute flow through
 * RequestContext.buildOpenapi(...) → RequestContext.build() → OperationContext.asSession(...) →
 * OperationContext.getEnrichment(Class).
 *
 * <p>Uses a local {@link SampleEnrichment} subclass so the OSS test does not depend on any
 * deployment-specific enrichment concept.
 */
public class EnrichmentPropagationTest {

  /** A locally-declared enrichment purely for this test's plumbing verification. */
  private record SampleEnrichment(String value) implements Enrichment {}

  /**
   * A second sample enrichment — proves multi-type coexistence in one EnrichmentBundle container.
   */
  private record OtherSampleEnrichment(String name) implements Enrichment {}

  /**
   * Forces a hashCode collision (constant 42) so two distinct values share a Java hash — used to
   * prove the context cache id discriminates by content, not by the collision-prone hashCode.
   */
  private record ConstantHashEnrichment(String value) implements Enrichment {
    @Override
    public int hashCode() {
      return 42;
    }
  }

  @Test
  public void enrichmentsFlowFromRequestAttributeToSessionOpContext() {
    // Arrange: system context + request bearing enrichments on the standard attribute.
    OperationContext systemCtx = TestOperationContexts.systemContextNoSearchAuthorization();
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/openapi/some/endpoint");
    request.setRemoteAddr("10.0.0.1");
    SampleEnrichment sample = new SampleEnrichment("propagated");
    request.setAttribute(RequestContext.REQUEST_ENRICHMENTS_ATTR, EnrichmentBundle.of(sample));

    Authentication auth = new Authentication(new Actor(ActorType.USER, "tester"), "");

    // Act: build a session opContext via the standard entrance-point path.
    OperationContext session =
        OperationContext.asSession(
            systemCtx,
            RequestContext.builder()
                .buildOpenapi(auth.getActor().toUrnStr(), request, "sampleAction", (String) null),
            Authorizer.EMPTY,
            auth,
            true);

    // Assert: typed retrieval works, and returns the exact instance that was stamped.
    Optional<SampleEnrichment> retrieved = session.getEnrichment(SampleEnrichment.class);
    assertTrue(retrieved.isPresent(), "SampleEnrichment must propagate to session opContext");
    assertSame(retrieved.get(), sample, "Same instance should propagate, no copy");
    assertEquals(retrieved.get().value(), "propagated");

    // The bare EnrichmentBundle container is also exposed via the raw getter.
    EnrichmentBundle container = session.getEnrichmentBundle();
    assertNotNull(container);
    assertTrue(container.get(SampleEnrichment.class).isPresent());
  }

  @Test
  public void noAttributeSet_sessionOpContextHasNoEnrichmentBundle() {
    OperationContext systemCtx = TestOperationContexts.systemContextNoSearchAuthorization();
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/openapi/some/endpoint");
    request.setRemoteAddr("10.0.0.1");
    Authentication auth = new Authentication(new Actor(ActorType.USER, "tester"), "");

    OperationContext session =
        OperationContext.asSession(
            systemCtx,
            RequestContext.builder()
                .buildOpenapi(auth.getActor().toUrnStr(), request, "sampleAction", (String) null),
            Authorizer.EMPTY,
            auth,
            true);

    // getEnrichment on a missing type returns Optional.empty rather than throwing.
    assertTrue(session.getEnrichment(SampleEnrichment.class).isEmpty());
  }

  @Test
  public void emptyEnrichmentBundleAttribute_treatedAsAbsent() {
    // Guards against the case where a filter constructs EnrichmentBundle.EMPTY and stashes it — we
    // don't want that to shadow a possibly-stampeable default down the chain.
    OperationContext systemCtx = TestOperationContexts.systemContextNoSearchAuthorization();
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/openapi/some/endpoint");
    request.setRemoteAddr("10.0.0.1");
    request.setAttribute(RequestContext.REQUEST_ENRICHMENTS_ATTR, EnrichmentBundle.EMPTY);
    Authentication auth = new Authentication(new Actor(ActorType.USER, "tester"), "");

    OperationContext session =
        OperationContext.asSession(
            systemCtx,
            RequestContext.builder()
                .buildOpenapi(auth.getActor().toUrnStr(), request, "sampleAction", (String) null),
            Authorizer.EMPTY,
            auth,
            true);

    assertTrue(session.getEnrichment(SampleEnrichment.class).isEmpty());
  }

  @Test
  public void addEnrichment_mergesWithExistingAttribute() {
    // RequestContext.addEnrichment must NOT clobber an existing enrichment stamped by another
    // filter — must merge. Guards multi-filter safety.
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/openapi/x");
    request.setRemoteAddr("10.0.0.1");
    request.setAttribute(
        RequestContext.REQUEST_ENRICHMENTS_ATTR,
        EnrichmentBundle.of(new OtherSampleEnrichment("preExisting")));

    RequestContext.addEnrichment(request, new SampleEnrichment("added"));

    EnrichmentBundle after =
        (EnrichmentBundle) request.getAttribute(RequestContext.REQUEST_ENRICHMENTS_ATTR);
    assertTrue(after.get(OtherSampleEnrichment.class).isPresent());
    assertTrue(after.get(SampleEnrichment.class).isPresent());
    assertEquals(after.get(OtherSampleEnrichment.class).get().name(), "preExisting");
    assertEquals(after.get(SampleEnrichment.class).get().value(), "added");
  }

  @Test
  public void addEnrichment_sameClassTwice_lastWriterWins() {
    // Filter A stamps SampleEnrichment("first"). Filter B stamps SampleEnrichment("second").
    // The second wins — a filter is authoritative for its own enrichment type.
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/openapi/x");
    request.setRemoteAddr("10.0.0.1");

    RequestContext.addEnrichment(request, new SampleEnrichment("first"));
    RequestContext.addEnrichment(request, new SampleEnrichment("second"));

    EnrichmentBundle after =
        (EnrichmentBundle) request.getAttribute(RequestContext.REQUEST_ENRICHMENTS_ATTR);
    assertEquals(after.get(SampleEnrichment.class).get().value(), "second");
  }

  @Test
  public void addEnrichment_bootstrapsEmptyAttribute() {
    // No preceding filter — addEnrichment must create the container, not NPE.
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/openapi/x");
    request.setRemoteAddr("10.0.0.1");

    RequestContext.addEnrichment(request, new SampleEnrichment("bootstrap"));

    EnrichmentBundle after =
        (EnrichmentBundle) request.getAttribute(RequestContext.REQUEST_ENRICHMENTS_ATTR);
    assertNotNull(after);
    assertEquals(after.get(SampleEnrichment.class).get().value(), "bootstrap");
  }

  @Test
  public void withEnrichment_copyOnWrite_enhancesExistingEnrichment() {
    // Downstream service reads an enrichment, produces an enhanced record via with...(),
    // and stitches back via opContext.withEnrichment(...). Original context is unchanged.
    OperationContext systemCtx = TestOperationContexts.systemContextNoSearchAuthorization();
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/openapi/x");
    request.setRemoteAddr("10.0.0.1");
    request.setAttribute(
        RequestContext.REQUEST_ENRICHMENTS_ATTR, EnrichmentBundle.of(new SampleEnrichment("v1")));
    Authentication auth = new Authentication(new Actor(ActorType.USER, "tester"), "");

    OperationContext session =
        OperationContext.asSession(
            systemCtx,
            RequestContext.builder()
                .buildOpenapi(auth.getActor().toUrnStr(), request, "x", (String) null),
            Authorizer.EMPTY,
            auth,
            true);

    // Simulate "enhance": create a v2 record and stitch back.
    OperationContext enhanced = session.withEnrichment(new SampleEnrichment("v2"));

    assertEquals(session.getEnrichment(SampleEnrichment.class).get().value(), "v1");
    assertEquals(enhanced.getEnrichment(SampleEnrichment.class).get().value(), "v2");
    // Original session context is untouched.
    assertEquals(session.getEnrichment(SampleEnrichment.class).get().value(), "v1");
  }

  @Test
  public void withEnrichment_onNullBundleContext_bootstrapsBundle() {
    // A context with no enrichments (typical bootstrap / system context) must accept
    // withEnrichment without NPE — the getEnrichmentBundle() @Nonnull getter closes the footgun
    // even when the backing field is null. Without the getter's default, this path would NPE on
    // the internal .plus(...) call.
    OperationContext systemCtx = TestOperationContexts.systemContextNoSearchAuthorization();
    assertTrue(systemCtx.getEnrichment(SampleEnrichment.class).isEmpty());

    OperationContext enhanced = systemCtx.withEnrichment(new SampleEnrichment("bootstrapped"));

    assertEquals(enhanced.getEnrichment(SampleEnrichment.class).get().value(), "bootstrapped");
    // Original stays clean.
    assertTrue(systemCtx.getEnrichment(SampleEnrichment.class).isEmpty());
  }

  @Test
  public void enrichmentDiscriminatesContextCacheIds() {
    // The enrichment bundle must discriminate the OperationContext cache ids, so a cache keyed on
    // getSearchContextId()/getEntityContextId()/getGlobalContextId() (e.g. the entity-search cache)
    // isolates by enrichment. This is the generic mechanism behind per-operation cache isolation: a
    // deployment stamps an enrichment and two operations carrying different enrichments can no
    // longer collide on a cache key.
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    OperationContext a = base.withEnrichment(new SampleEnrichment("a"));
    OperationContext b = base.withEnrichment(new SampleEnrichment("b"));

    // Adding an enrichment changes every id scope vs the bare context.
    assertNotEquals(base.getSearchContextId(), a.getSearchContextId());
    assertNotEquals(base.getEntityContextId(), a.getEntityContextId());
    assertNotEquals(base.getGlobalContextId(), a.getGlobalContextId());

    // Different enrichment value -> different ids in every scope.
    assertNotEquals(a.getSearchContextId(), b.getSearchContextId());
    assertNotEquals(a.getEntityContextId(), b.getEntityContextId());
    assertNotEquals(a.getGlobalContextId(), b.getGlobalContextId());
  }

  @Test
  public void sameEnrichmentValueYieldsStableContextCacheIds() {
    // Stability matters as much as discrimination: two contexts carrying an equal enrichment must
    // produce identical ids, otherwise the cache would never hit for a given enrichment value.
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    OperationContext first = base.withEnrichment(new SampleEnrichment("same"));
    OperationContext second = base.withEnrichment(new SampleEnrichment("same"));

    assertEquals(first.getSearchContextId(), second.getSearchContextId());
    assertEquals(first.getEntityContextId(), second.getEntityContextId());
    assertEquals(first.getGlobalContextId(), second.getGlobalContextId());
  }

  @Test
  public void hashCollidingEnrichmentsStillYieldDistinctCacheIds() {
    // Two enrichments with equal hashCodes but different values. A hashCode-based discriminator
    // would collapse them onto one cache id (serving one identity's data to the other); the stable
    // content token must keep them distinct.
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    ConstantHashEnrichment x = new ConstantHashEnrichment("x");
    ConstantHashEnrichment y = new ConstantHashEnrichment("y");
    assertEquals(x.hashCode(), y.hashCode(), "test premise: hashCodes must collide");

    OperationContext cx = base.withEnrichment(x);
    OperationContext cy = base.withEnrichment(y);

    assertNotEquals(cx.getSearchContextId(), cy.getSearchContextId());
    assertNotEquals(cx.getEntityContextId(), cy.getEntityContextId());
    assertNotEquals(cx.getGlobalContextId(), cy.getGlobalContextId());
  }

  @Test
  public void plus_returnsNewInstance_originalUnchanged() {
    EnrichmentBundle original = EnrichmentBundle.of(new SampleEnrichment("a"));

    EnrichmentBundle extended = original.plus(new OtherSampleEnrichment("b"));

    // Original stays 1-entry.
    assertTrue(original.get(OtherSampleEnrichment.class).isEmpty());
    // Derived has both.
    assertEquals(extended.get(SampleEnrichment.class).get().value(), "a");
    assertEquals(extended.get(OtherSampleEnrichment.class).get().name(), "b");
  }

  @Test
  public void wrongTypedAttribute_silentlyIgnored() {
    // Guards against a caller stashing the wrong shape (e.g. a raw Map). We don't want to NPE
    // or ClassCastException here — enrichments are best-effort propagation.
    OperationContext systemCtx = TestOperationContexts.systemContextNoSearchAuthorization();
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/openapi/some/endpoint");
    request.setRemoteAddr("10.0.0.1");
    request.setAttribute(
        RequestContext.REQUEST_ENRICHMENTS_ATTR, "not an EnrichmentBundle instance");
    Authentication auth = new Authentication(new Actor(ActorType.USER, "tester"), "");

    OperationContext session =
        OperationContext.asSession(
            systemCtx,
            RequestContext.builder()
                .buildOpenapi(auth.getActor().toUrnStr(), request, "sampleAction", (String) null),
            Authorizer.EMPTY,
            auth,
            true);

    assertTrue(session.getEnrichment(SampleEnrichment.class).isEmpty());
  }
}
