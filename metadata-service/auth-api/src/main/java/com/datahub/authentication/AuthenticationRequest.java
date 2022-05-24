package com.datahub.authentication;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;


/**
 * Request context provided to each {@link Authenticator} to perform Authentication.
 *
 * Currently, this class only hold the inbound request's headers, but could certainly be extended
 * to contain additional information like the request parameters, body, ip, etc as needed.
 */
public class AuthenticationRequest {

  private final Map<String, String> caseInsensitiveHeaders;

  public AuthenticationRequest(@Nonnull final Map<String, String> requestHeaders) {
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
