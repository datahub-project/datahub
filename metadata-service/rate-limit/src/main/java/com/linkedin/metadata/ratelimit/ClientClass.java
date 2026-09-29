package com.linkedin.metadata.ratelimit;

/**
 * Coarse client classification used to narrow rate-limit rule candidates.
 *
 * <p><b>Advisory, not security.</b> The signal is the {@code X-DataHub-Request-Source} header
 * stamped by the datahub-frontend proxy and read via {@link ClientClassifier} — trustworthy on the
 * frontend-proxied hop (Play strips any client-supplied value and re-stamps it from a signed
 * session cookie), but a caller reaching GMS directly can set it, so it is advisory. Browser
 * classification only ever <i>loosens</i> limits for UI traffic; the class-agnostic global cap
 * always applies regardless of class, so a spoofed value can never escape the global protection,
 * and class buckets apply only when {@code RATE_LIMITS_CLIENT_CLASS_ENABLED=true}. Unknown or blank
 * values resolve to {@link #NON_BROWSER} (the tighter tier).
 *
 * <p>Intentionally coarse for v1. Fine-grained types (sdk/cli/ingestion) are a non-breaking future
 * extension via the rule {@code clientTypes} list — no engine change required.
 */
public enum ClientClass {
  BROWSER,
  NON_BROWSER
}
