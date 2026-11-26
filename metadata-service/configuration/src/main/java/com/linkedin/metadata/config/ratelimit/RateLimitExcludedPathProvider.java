package com.linkedin.metadata.config.ratelimit;

import java.util.Set;

/**
 * Extra paths the REST rate limiter leaves alone, contributed in code by the component that serves
 * them, alongside the configured {@code excludedPaths}. For a path whose caller has no DataHub
 * identity of its own, which would otherwise share one anonymous bucket with every other such
 * caller. A path is excluded exactly while a provider names it.
 *
 * <p>Paths match {@code HttpServletRequest#getServletPath()} exactly; there is no wildcard form.
 */
public interface RateLimitExcludedPathProvider {

  /** Never null. Asked on every request, so the set may change after startup. */
  Set<String> excludedPaths();
}
