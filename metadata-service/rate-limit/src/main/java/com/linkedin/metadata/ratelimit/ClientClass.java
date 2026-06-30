package com.linkedin.metadata.ratelimit;

/**
 * Coarse client classification used to narrow rate-limit rule candidates.
 *
 * <p><b>Advisory, not security.</b> The signal is the request {@code User-Agent} (via {@code
 * RequestContext.agentClass}), which is client-supplied and trivially spoofable. Browser
 * classification only ever <i>loosens</i> limits for UI traffic; the class-agnostic global cap
 * always applies regardless of class, so a spoofed browser UA can never escape the global
 * protection. Unknown or blank agents resolve to {@link #NON_BROWSER} (the tighter tier).
 *
 * <p>Intentionally coarse for v1. Fine-grained types (sdk/cli/ingestion) are a non-breaking future
 * extension via the rule {@code clientTypes} list — no engine change required.
 */
public enum ClientClass {
  BROWSER,
  NON_BROWSER
}
