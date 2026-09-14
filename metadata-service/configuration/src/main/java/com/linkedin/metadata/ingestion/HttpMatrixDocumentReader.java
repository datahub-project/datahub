package com.linkedin.metadata.ingestion;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import javax.annotation.Nullable;

/**
 * Reads the matrix document over HTTP / HTTPS — a CDN, a GitHub raw URL, a gist, or any plain file
 * host. Suitable for any deployment that wants to change connector versions without rebuilding or
 * redeploying GMS.
 *
 * <p>Uses {@link java.net.http.HttpClient}, which is HTTP/HTTPS-only by design, so non-HTTP schemes
 * ({@code file://}, {@code jar://}, {@code ftp://}, …) are rejected at request-send time.
 *
 * <p>For private hosts (e.g. a private GitHub repo's {@code raw.githubusercontent.com} URL) an
 * optional {@code authHeader} value is sent verbatim as the {@code Authorization} request header.
 * Format is whatever the host expects — e.g. {@code "token ghp_xxx"} for a GitHub PAT, {@code
 * "Bearer ey..."} for an OIDC token. When {@code authHeader} is {@code null} or empty no auth
 * header is sent (the public-URL path is unchanged).
 */
public class HttpMatrixDocumentReader implements MatrixDocumentReader {

  private static final int FETCH_TIMEOUT_MS = 10_000;

  private final String url;
  @Nullable private final String authHeader;
  private final HttpClient httpClient;

  /** Convenience constructor for unauthenticated (public) URLs. */
  public HttpMatrixDocumentReader(String url) {
    this(url, null);
  }

  public HttpMatrixDocumentReader(String url, @Nullable String authHeader) {
    this.url = url;
    this.authHeader = authHeader;
    this.httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.ofMillis(FETCH_TIMEOUT_MS)).build();
  }

  @Override
  public String displayUri() {
    return url;
  }

  @Override
  public String read() throws Exception {
    HttpRequest.Builder reqBuilder =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofMillis(FETCH_TIMEOUT_MS))
            .header("User-Agent", "DataHub-GMS")
            // Lets the URL point at the GitHub "contents" API
            // (https://api.github.com/repos/<org>/<repo>/contents/<path>?ref=<branch>) — the only
            // authenticated way to read a file from a private/internal GitHub repo, since
            // raw.githubusercontent.com does not honor the Authorization header for those. With
            // this Accept the contents API returns the raw file body instead of base64 JSON.
            // Plain file hosts (raw URLs, gists, S3, CDNs) ignore an unknown Accept and still
            // return the file, so sending it unconditionally is safe for public URLs too.
            .header("Accept", "application/vnd.github.raw")
            .GET();
    if (authHeader != null && !authHeader.isEmpty()) {
      reqBuilder.header("Authorization", authHeader);
    }
    // Connect/read timeouts, DNS and TLS failures, and a malformed URL all propagate as-is: there
    // is no status code to classify on, so the polling source reports them as transport.
    HttpResponse<String> response =
        httpClient.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() / 100 != 2) {
      // Classify the status so a 401/403 reads as "fix access" and a 404 as "fix the URL", rather
      // than leaving an operator to interpret a bare status code.
      throw new MatrixReadException(
          MatrixRefreshFailure.forHttpStatus(response.statusCode()),
          "Non-2xx response fetching " + url + ": HTTP " + response.statusCode());
    }
    return response.body();
  }
}
