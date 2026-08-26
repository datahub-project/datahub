package com.linkedin.r2.transport.http.server;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.datahub.context.Enrichment;
import com.datahub.context.EnrichmentBundle;
import com.linkedin.r2.message.RequestContext;
import org.springframework.mock.web.MockHttpServletRequest;
import org.testng.annotations.Test;

/**
 * Direct coverage of {@link JakartaServletHelper#readRequestContext} — the bridge that copies the
 * enrichment attribute from an {@link jakarta.servlet.http.HttpServletRequest} onto the R2 {@link
 * RequestContext#getLocalAttr} space so downstream Rest.li endpoints can read it via {@code
 * resourceContext.getRawRequestContext().getLocalAttr(...)}.
 *
 * <p>This is the only production code that owns servlet→R2 attribute propagation. If it silently
 * stops copying, every Rest.li endpoint loses whatever enrichment the servlet filter chain stamped
 * — including tenant routing in the cloud fork. Cloud E2E tests seed the R2 attr directly, so they
 * can't catch a regression here.
 */
public class JakartaServletHelperEnrichmentTest {

  private static final String KEY =
      io.datahubproject.metadata.context.RequestContext.REQUEST_ENRICHMENTS_ATTR;

  @Test
  public void bundleOnServletRequest_isCopiedIntoR2LocalAttrs() {
    MockHttpServletRequest req = new MockHttpServletRequest("GET", "/aspects/urn:li:dataset:foo");
    req.setRemoteAddr("10.0.0.1");
    SampleEnrichment sample = new SampleEnrichment("propagated");
    EnrichmentBundle bundle = EnrichmentBundle.of(sample);
    req.setAttribute(KEY, bundle);

    RequestContext r2Context = JakartaServletHelper.readRequestContext(req);

    // Same instance — no copy/serialize, direct reference propagation.
    assertSame(r2Context.getLocalAttr(KEY), bundle);
  }

  @Test
  public void noBundleOnServletRequest_r2LocalAttrIsAbsent() {
    // Guards the null-check in JakartaServletHelper — without it, we'd write a null attr and
    // downstream Rest.li reads would see an unexpected sentinel.
    MockHttpServletRequest req = new MockHttpServletRequest("GET", "/aspects/urn:li:dataset:foo");
    req.setRemoteAddr("10.0.0.1");

    RequestContext r2Context = JakartaServletHelper.readRequestContext(req);

    assertNull(r2Context.getLocalAttr(KEY));
  }

  private record SampleEnrichment(String value) implements Enrichment {}
}
