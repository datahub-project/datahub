package io.datahubproject.test.boot;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.springframework.context.ApplicationContext;

/**
 * Checks that servlets and controllers are actually mounted on the embedded server. A route counts
 * as mounted when it answers with anything but 404: an unauthenticated request getting 401 proves
 * the route and the auth filter chain both exist, which is what a wiring test needs.
 */
public final class HttpRoutes {

  private static final HttpClient CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  private HttpRoutes() {}

  /** Each entry is {@code "<METHOD> <path>"}, e.g. {@code "POST /api/graphql"}. */
  public static BiConsumer<ApplicationContext, List<String>> mounted(@Nonnull String... routes) {
    return (ctx, v) -> {
      Integer port = port(ctx);
      if (port == null) {
        v.add("no embedded web server; cannot probe " + List.of(routes));
        return;
      }
      for (String route : routes) {
        String[] parts = route.split(" ", 2);
        try {
          int status = status(port, parts[0], parts[1]);
          if (status == 404) {
            v.add("route not mounted (404): " + route);
          }
        } catch (IOException | InterruptedException e) {
          v.add("route " + route + " failed: " + e);
        }
      }
    };
  }

  /** {@code "<METHOD> <path>" -> expected status}, for routes whose exact answer matters. */
  public static BiConsumer<ApplicationContext, List<String>> answers(
      @Nonnull String route, int expectedStatus) {
    return (ctx, v) -> {
      Integer port = port(ctx);
      if (port == null) {
        v.add("no embedded web server; cannot probe " + route);
        return;
      }
      String[] parts = route.split(" ", 2);
      try {
        int status = status(port, parts[0], parts[1]);
        if (status != expectedStatus) {
          v.add(route + " answered " + status + ", expected " + expectedStatus);
        }
      } catch (IOException | InterruptedException e) {
        v.add("route " + route + " failed: " + e);
      }
    };
  }

  /** Spring Boot publishes the bound port of the main web server as {@code local.server.port}. */
  private static Integer port(ApplicationContext ctx) {
    return ctx.getEnvironment().getProperty("local.server.port", Integer.class);
  }

  private static int status(int port, String method, String path)
      throws IOException, InterruptedException {
    HttpRequest.BodyPublisher body =
        "GET".equals(method)
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString("{}");
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .timeout(Duration.ofSeconds(30))
            .header("Content-Type", "application/json")
            .method(method, body)
            .build();
    return CLIENT.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
  }
}
