package utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class ProductUpdateHttpTest {

  @Test
  public void testFollowsRedirectToTheJsonBody() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    int port = server.getAddress().getPort();
    String origin = "http://127.0.0.1:" + port;
    server.createContext(
        "/updates.json",
        exchange -> {
          byte[] body = "{\"enabled\":true,\"id\":\"v2.3\"}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.createContext(
        "/updates",
        exchange -> {
          exchange.getResponseHeaders().add("Location", origin + "/updates.json");
          exchange.sendResponseHeaders(307, -1);
          exchange.close();
        });
    server.start();
    try {
      var response = ProductUpdateHttp.get(origin + "/updates");
      assertEquals(200, response.statusCode());
      assertTrue(response.body().contains("\"id\":\"v2.3\""));
    } finally {
      server.stop(0);
    }
  }
}
