package com.datahub.authentication;

import com.datahub.plugins.auth.authentication.Authenticator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Request context provided to each {@link Authenticator} to perform Authentication.
 *
 * <p>Currently, this class only hold the inbound request's headers, but could certainly be extended
 * to contain additional information like the request parameters, body, ip, etc as needed.
 */
@Getter
public class AuthenticationRequest {

  private final Map<String, String> caseInsensitiveHeaders;

  private final String servletInfo;
  private final String pathInfo;

  public AuthenticationRequest(@Nonnull final Map<String, String> requestHeaders) {
    this("", "", requestHeaders);
  }

  public AuthenticationRequest(
      @Nonnull String servletInfo,
      @Nonnull String pathInfo,
      @Nonnull final Map<String, String> requestHeaders) {
    Objects.requireNonNull(requestHeaders);
    caseInsensitiveHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    caseInsensitiveHeaders.putAll(requestHeaders);
    this.servletInfo = servletInfo;
    this.pathInfo = pathInfo;
  }

  /**
   * @return Returns a case-insensitive map of the inbound request's headers.
   */
  @Nonnull
  public Map<String, String> getRequestHeaders() {
    return this.caseInsensitiveHeaders;
  }
}
