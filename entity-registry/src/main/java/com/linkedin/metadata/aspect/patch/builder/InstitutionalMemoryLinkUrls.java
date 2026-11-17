package com.linkedin.metadata.aspect.patch.builder;

import java.net.URI;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Schemes accepted on institutional-memory links. Shared by the patch builder and GraphQL so a
 * {@code javascript:} or {@code data:} URL cannot be stored through either API.
 */
public final class InstitutionalMemoryLinkUrls {

  public static final Set<String> ALLOWED_SCHEMES = Set.of("http", "https", "ftp", "mailto");

  private InstitutionalMemoryLinkUrls() {}

  public static void validate(@Nonnull String url) {
    final String scheme;
    try {
      scheme = URI.create(url).getScheme();
    } catch (Exception e) {
      throw new IllegalArgumentException("Expected a valid URL.");
    }
    if (scheme == null || !ALLOWED_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT))) {
      throw new IllegalArgumentException("URL scheme '" + scheme + "' is not allowed.");
    }
  }
}
