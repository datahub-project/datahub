package com.datahub.authentication;

import java.util.Set;

/**
 * Extra paths the authentication enforcement filter leaves to someone else, contributed in code by
 * whichever component authenticates them. Unlike the {@code authentication.excludedPaths} setting,
 * a provider names a path only while its own check guards it.
 *
 * <p>Paths match {@code HttpServletRequest#getServletPath()} exactly. There is no wildcard form, so
 * a provider cannot open more than the paths it names.
 */
public interface AuthenticationExcludedPathProvider {

  /** Never null. Asked on every request, so the set may change after startup. */
  Set<String> excludedPaths();
}
