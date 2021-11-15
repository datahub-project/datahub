package com.datahub.authentication;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;


/**
 * Request context provided to each {@link Authenticator} during authentication.
 */
public class AuthenticationContext {

  private final Map<String, String> caseInsensitiveHeaders;

  public AuthenticationContext(@Nonnull final Map<String, String> requestHeaders) {
    Objects.requireNonNull(requestHeaders);
    caseInsensitiveHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    caseInsensitiveHeaders.putAll(requestHeaders);
  }

  /**
   * Returns a case-insensitive map of the inbound request's headers.
   */
  @Nonnull
  public Map<String, String> getRequestHeaders() {
    return this.caseInsensitiveHeaders;
  }
}
