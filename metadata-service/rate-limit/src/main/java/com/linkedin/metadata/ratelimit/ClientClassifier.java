package com.linkedin.metadata.ratelimit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps a request's frontend-stamped origin header to a coarse {@link ClientClass}.
 *
 * <p><b>Advisory, not security</b> (see {@link ClientClass}).
 */
public final class ClientClassifier {

  /**
   * Header stamped by datahub-frontend on every proxied request, carrying the request's origin.
   * This string is a cross-service wire contract — keep it in sync with the frontend's value in
   * {@code Application.java}.
   */
  public static final String REQUEST_SOURCE_HEADER = "X-DataHub-Request-Source";

  private static final String SOURCE_BROWSER = "BROWSER";

  private ClientClassifier() {}

  /**
   * Primary classification path: maps the frontend-stamped {@link #REQUEST_SOURCE_HEADER} to a
   * {@link ClientClass}. {@code "BROWSER"} (an authenticated UI session — Play strips any
   * client-supplied value and re-stamps it from a signed session cookie, so it is trustworthy on
   * the frontend-proxied hop) → {@link ClientClass#BROWSER}; anything else (absent, blank, {@code
   * "SDK"}, or any direct-to-GMS request) → {@link ClientClass#NON_BROWSER}, the safe/tight
   * default. Advisory only: a caller reaching GMS directly can set this header, so class buckets
   * apply only when {@code RATE_LIMITS_CLIENT_CLASS_ENABLED=true} (see {@link ClientClass}).
   *
   * <p>Generic by design — call it from any enforcement point (GraphQL gate, servlet filter) with
   * the raw header value; the caller only needs {@code request.getHeader(REQUEST_SOURCE_HEADER)}.
   */
  @Nonnull
  public static ClientClass fromRequestSource(@Nullable String requestSourceHeader) {
    return SOURCE_BROWSER.equalsIgnoreCase(requestSourceHeader)
        ? ClientClass.BROWSER
        : ClientClass.NON_BROWSER;
  }
}
