package controllers;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.util.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import play.http.HttpEntity;
import play.mvc.Http;
import play.mvc.Result;

class AiServiceControllerTest {

  /** One request as datahub-ai-service saw it. */
  record Seen(String method, String path, Map<String, List<String>> headers, String body) {}

  private HttpServer upstream;
  private final List<Seen> seen = new CopyOnWriteArrayList<>();
  private volatile CountDownLatch releaseStream = new CountDownLatch(0);
  private ActorSystem system;
  private Materializer mat;

  @BeforeEach
  void setUp() throws IOException {
    upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
    upstream.createContext("/", this::handle);
    upstream.start();
    // Reference config only: application.conf needs secrets a unit test has no use for.
    system = ActorSystem.create("ai-proxy-test", ConfigFactory.defaultReference());
    mat = Materializer.matFromSystem(system);
  }

  @AfterEach
  void tearDown() {
    releaseStream.countDown();
    upstream.stop(0);
    system.terminate();
  }

  // Streams two SSE events, the second only after the test releases it.
  private void handle(HttpExchange exchange) throws IOException {
    seen.add(
        new Seen(
            exchange.getRequestMethod(),
            exchange.getRequestURI().getPath(),
            new HashMap<>(exchange.getRequestHeaders()),
            new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
    exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
    exchange.getResponseHeaders().add("Mcp-Session-Id", "abc");
    exchange.getResponseHeaders().add("Connection", "close");
    exchange.sendResponseHeaders(200, 0);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write("event: message\ndata: one\n\n".getBytes(StandardCharsets.UTF_8));
      out.flush();
      try {
        releaseStream.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      out.write("event: message\ndata: two\n\n".getBytes(StandardCharsets.UTF_8));
    } catch (IOException ignored) {
      // The client went away.
    }
  }

  private AiServiceController controller(Map<String, Object> overrides) {
    Map<String, Object> config = new HashMap<>();
    config.put(
        AiServiceController.URL_CONFIG, "http://127.0.0.1:" + upstream.getAddress().getPort());
    config.putAll(overrides);
    Config parsed = ConfigFactory.parseMap(config);
    return new AiServiceController(parsed);
  }

  private static Http.Request post(String uri) {
    return new Http.RequestBuilder()
        .method("POST")
        .uri(uri)
        .header("Host", "localhost:9002")
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .bodyRaw(ByteString.fromString("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}"))
        .build();
  }

  private Result call(AiServiceController controller, String path, Http.Request request)
      throws Exception {
    return controller.proxyMcp(path, request).toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  private String body(Result result) throws Exception {
    return result
        .body()
        .consumeData(mat)
        .toCompletableFuture()
        .get(10, TimeUnit.SECONDS)
        .utf8String();
  }

  @Test
  void notFoundWithoutAServiceUrl() throws Exception {
    AiServiceController controller = new AiServiceController(ConfigFactory.empty());

    assertEquals(404, call(controller, "", post("/mcp")).status());
  }

  @Test
  void tokenInTheUrlIsRefused() throws Exception {
    Result result = call(controller(Map.of()), "", post("/mcp?token=secret"));

    assertEquals(400, result.status());
    assertTrue(seen.isEmpty());
  }

  @Test
  void preflightIsRefusedWithoutCorsHeaders() {
    Result result = controller(Map.of()).rejectPreflight("");

    assertEquals(405, result.status());
    assertTrue(
        result.headers().keySet().stream()
            .noneMatch(name -> name.toLowerCase().startsWith("access-control-")));
  }

  @Test
  void forwardsTheCallerButNotTheSession() throws Exception {
    Http.Request request =
        new Http.RequestBuilder()
            .method("POST")
            .uri("/mcp/sub")
            .header("Host", "localhost:9002")
            .header("Authorization", "Bearer pat")
            .header("Origin", "http://localhost:9002")
            .header("Cookie", "PLAY_SESSION=secret")
            .header("X-DataHub-Actor", "urn:li:corpuser:admin")
            .header("X-Forwarded-Host", "attacker.example")
            .header("MCP-Protocol-Version", "2026-07-28")
            .header("Mcp-Method", "tools/call")
            .header("Mcp-Name", "get_me")
            .header("Mcp-Param-Urn", "urn:li:corpuser:x")
            .header("Content-Type", "application/json")
            .bodyRaw(ByteString.fromString("{}"))
            .build();
    releaseStream = new CountDownLatch(0);

    Result result = call(controller(Map.of()), "sub", request);
    body(result);

    Seen upstreamSaw = seen.get(0);
    Map<String, List<String>> headers = upstreamSaw.headers();
    assertEquals("/mcp/sub", upstreamSaw.path());
    assertEquals("{}", upstreamSaw.body());
    assertEquals(List.of("Bearer pat"), headers.get("Authorization"));
    assertEquals(List.of("http://localhost:9002"), headers.get("Origin"));
    // Every Mcp-* header: the stateless protocol routes on Mcp-Method / Mcp-Name / Mcp-Param-*.
    assertEquals(List.of("2026-07-28"), headers.get("Mcp-protocol-version"));
    assertEquals(List.of("tools/call"), headers.get("Mcp-method"));
    assertEquals(List.of("get_me"), headers.get("Mcp-name"));
    assertEquals(List.of("urn:li:corpuser:x"), headers.get("Mcp-param-urn"));
    // The client's own X-Forwarded-Host is overwritten with the Host it used.
    assertEquals(List.of("localhost:9002"), headers.get("X-forwarded-host"));
    assertNull(headers.get("Cookie"));
    assertNull(headers.get("X-datahub-actor"));
  }

  @Test
  void streamsWithoutBufferingAndKeepsResponseHeaders() throws Exception {
    releaseStream = new CountDownLatch(1);
    Result result = call(controller(Map.of()), "", post("/mcp"));

    // The first event arrives while the upstream is still holding the second back.
    HttpEntity.Streamed entity = (HttpEntity.Streamed) result.body();
    var first =
        entity
            .dataStream()
            .runWith(Sink.head(), mat)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertTrue(first.utf8String().contains("data: one"));
    assertEquals("abc", result.header("Mcp-Session-Id").orElse(null));
    assertEquals("identity", result.header("Content-Encoding").orElse(null));
    assertTrue(result.header("Connection").isEmpty());
    assertEquals("text/event-stream", entity.contentType().orElse(""));
  }

  @Test
  void holdsItsPermitUntilTheStreamEndsThenReleasesIt() throws Exception {
    AiServiceController controller =
        controller(Map.of(AiServiceController.MAX_IN_FLIGHT_CONFIG, 1));
    releaseStream = new CountDownLatch(1);

    Result streaming = call(controller, "", post("/mcp"));
    // Headers are back but the body is still streaming: the one permit is taken.
    assertEquals(503, call(controller, "", post("/mcp")).status());

    releaseStream.countDown();
    assertTrue(body(streaming).contains("data: two"));
    assertEquals(200, call(controller, "", post("/mcp")).status());
  }

  @Test
  void aClientThatGoesAwayReleasesThePermit() throws Exception {
    AiServiceController controller =
        controller(Map.of(AiServiceController.MAX_IN_FLIGHT_CONFIG, 1));
    releaseStream = new CountDownLatch(1);

    Result streaming = call(controller, "", post("/mcp"));
    // Read one event and stop: the downstream cancels mid-stream.
    ((HttpEntity.Streamed) streaming.body())
        .dataStream()
        .runWith(Sink.head(), mat)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);

    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    int status = 503;
    while (status == 503 && System.nanoTime() < deadline) {
      Thread.sleep(50);
      Result next = call(controller, "", post("/mcp"));
      status = next.status();
      if (status == 200) {
        releaseStream.countDown();
        body(next);
      }
    }
    assertEquals(200, status);
  }

  @Test
  void upstreamDownIsAJsonRpcServiceUnavailable() throws Exception {
    upstream.stop(0);

    Result result = call(controller(Map.of()), "", post("/mcp"));

    assertEquals(503, result.status());
    assertTrue(body(result).contains("\"jsonrpc\":\"2.0\""));
  }
}
