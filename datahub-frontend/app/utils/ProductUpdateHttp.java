package utils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import javax.annotation.Nonnull;

/**
 * Fetches the hosted product-update JSON. Uses its own client so redirects are followed the same
 * way GMS follows them. The shared Play client does not follow redirects, and changing that would
 * affect the API proxy.
 */
public final class ProductUpdateHttp {
  static final HttpClient CLIENT =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .followRedirects(HttpClient.Redirect.NORMAL)
          .connectTimeout(Duration.ofSeconds(2))
          .build();

  private ProductUpdateHttp() {}

  @Nonnull
  public static HttpResponse<String> get(@Nonnull String url)
      throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(url.trim())).timeout(Duration.ofSeconds(2)).GET().build();
    return CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
