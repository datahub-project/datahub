package io.datahubproject.test.boot;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.context.ApplicationContext;

/**
 * Checks that servlets and controllers are mounted on the embedded server.
 *
 * <p>Probes authenticate as the system client, because the authentication filter answers 401 for
 * any anonymous request, mounted or not. A route counts as mounted when it answers with anything
 * but 404 or 401; a 400 or 500 for an empty request still proves the request reached a handler.
 * Each check first probes a random path and requires a 404, so a probe that cannot fail is
 * reported.
 */
public final class HttpRoutes {

  private static final HttpClient CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  private HttpRoutes() {}

  /**
   * Each entry is {@code "<METHOD> <path>"}, e.g. {@code "POST /api/graphql"}. {@code prefix} is
   * the servlet context path ({@code ""} for GMS, {@code "/gms"} for the MCE consumer), used for
   * the 404 control probe.
   */
  public static BiConsumer<ApplicationContext, List<String>> mounted(
      @Nonnull String prefix, @Nonnull String... routes) {
    return (ctx, v) -> {
      Integer port = port(ctx);
      String auth = systemAuthorization();
      if (port == null || auth == null) {
        v.add("cannot probe " + List.of(routes) + ": no web server or no system client secret");
        return;
      }
      String control = "GET " + prefix + "/boot-test-unmounted-" + UUID.randomUUID();
      int controlStatus = probe(port, control, auth, v);
      if (controlStatus != 404) {
        v.add(
            "control probe "
                + control
                + " answered "
                + controlStatus
                + " instead of 404; route probes would be vacuous");
        return;
      }
      for (String route : routes) {
        int status = probe(port, route, auth, v);
        if (status == 404 || status == 401) {
          v.add("route not mounted (" + status + "): " + route);
        }
      }
    };
  }

  /** Unauthenticated probe expecting an exact status, e.g. {@code GET /health -> 200}. */
  public static BiConsumer<ApplicationContext, List<String>> answers(
      @Nonnull String route, int expectedStatus) {
    return (ctx, v) -> {
      Integer port = port(ctx);
      if (port == null) {
        v.add("no embedded web server; cannot probe " + route);
        return;
      }
      int status = probe(port, route, null, v);
      if (status != expectedStatus) {
        v.add(route + " answered " + status + ", expected " + expectedStatus);
      }
    };
  }

  /** Spring Boot publishes the bound port of the main web server as {@code local.server.port}. */
  private static Integer port(ApplicationContext ctx) {
    return ctx.getEnvironment().getProperty("local.server.port", Integer.class);
  }

  /** DataHubSystemAuthenticator format; the secret is set for every Gradle test task. */
  @Nullable
  private static String systemAuthorization() {
    String secret = System.getenv("DATAHUB_SYSTEM_CLIENT_SECRET");
    if (secret == null || secret.isBlank()) {
      return null;
    }
    String clientId = System.getenv().getOrDefault("DATAHUB_SYSTEM_CLIENT_ID", "__datahub_system");
    return "Basic " + clientId + ":" + secret;
  }

  private static int probe(int port, String route, @Nullable String auth, List<String> v) {
    String[] parts = route.split(" ", 2);
    HttpRequest.BodyPublisher body =
        "GET".equals(parts[0])
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString("{}");
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + parts[1]))
            .timeout(Duration.ofSeconds(30))
            .header("Content-Type", "application/json")
            .method(parts[0], body);
    if (auth != null) {
      request.header("Authorization", auth);
    }
    try {
      return CLIENT.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    } catch (IOException e) {
      v.add("route " + route + " failed: " + e);
      return -1;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      v.add("route " + route + " interrupted");
      return -1;
    }
  }
}
