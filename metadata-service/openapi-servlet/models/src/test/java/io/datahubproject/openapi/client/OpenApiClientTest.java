package io.datahubproject.openapi.client;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.datahubproject.openapi.v2.models.BatchGetUrnRequestV2;
import io.datahubproject.openapi.v2.models.BatchGetUrnResponseV2;
import io.datahubproject.openapi.v2.models.GenericAspectV2;
import io.datahubproject.openapi.v2.models.GenericEntityV2;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenApiClientTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String URN_1 = "urn:li:dataHubExecutionRequest:one";
  private static final String URN_2 = "urn:li:dataHubExecutionRequest:two";

  private HttpServer server;
  private final AtomicReference<String> requestUri = new AtomicReference<>();
  private final AtomicReference<String> requestBody = new AtomicReference<>();
  private final AtomicReference<String> requestAuth = new AtomicReference<>();
  private final AtomicReference<String> responseBody = new AtomicReference<>();

  @BeforeMethod
  public void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          requestUri.set(exchange.getRequestURI().toString());
          requestAuth.set(exchange.getRequestHeaders().getFirst("Authorization"));
          requestBody.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] out = responseBody.get().getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, out.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(out);
          }
        });
    server.start();
  }

  @AfterMethod
  public void stopServer() {
    server.stop(0);
  }

  private OpenApiClient client() {
    return new OpenApiClient("localhost", server.getAddress().getPort(), false, null);
  }

  @Test
  public void testGetBatchUrnsCallsV3BatchGetAndMapsToV2Response() throws Exception {
    responseBody.set(
        "[{\"urn\":\""
            + URN_1
            + "\",\"dataHubExecutionRequestResult\":{\"value\":{\"status\":\"SUCCESS\"},"
            + "\"systemMetadata\":{\"runId\":\"r1\"}}},"
            + "{\"urn\":\""
            + URN_2
            + "\",\"dataHubExecutionRequestResult\":{\"value\":{\"status\":\"RUNNING\"}}}]");

    BatchGetUrnResponseV2<GenericAspectV2, GenericEntityV2> response =
        client()
            .getBatchUrns(
                "dataHubExecutionRequest",
                BatchGetUrnRequestV2.builder()
                    .urns(List.of(URN_1, URN_2))
                    .aspectNames(List.of("dataHubExecutionRequestResult"))
                    .withSystemMetadata(true)
                    .build(),
                "Bearer token");

    assertEquals(
        requestUri.get(),
        "/openapi/v3/entity/dataHubExecutionRequest/batchGet?systemMetadata=true");
    assertEquals(requestAuth.get(), "Bearer token");
    assertEquals(
        OBJECT_MAPPER.readValue(
            requestBody.get(), new TypeReference<List<Map<String, Object>>>() {}),
        List.of(
            Map.of("urn", URN_1, "dataHubExecutionRequestResult", Map.of()),
            Map.of("urn", URN_2, "dataHubExecutionRequestResult", Map.of())));

    List<GenericEntityV2> entities = response.getEntities();
    assertEquals(entities.size(), 2);
    assertEquals(entities.get(0).getUrn(), URN_1);
    GenericAspectV2 first = entities.get(0).getAspects().get("dataHubExecutionRequestResult");
    assertEquals(first.get("value"), Map.of("status", "SUCCESS"));
    assertEquals(first.get("systemMetadata"), Map.of("runId", "r1"));
    GenericAspectV2 second = entities.get(1).getAspects().get("dataHubExecutionRequestResult");
    assertEquals(second.get("value"), Map.of("status", "RUNNING"));
    assertFalse(second.containsKey("systemMetadata"));
  }

  @Test
  public void testEmptyAspectNamesRequestsAllAspects() throws Exception {
    responseBody.set("[{\"urn\":\"" + URN_1 + "\"}]");

    BatchGetUrnResponseV2<GenericAspectV2, GenericEntityV2> response =
        client()
            .getBatchUrns(
                "dataHubExecutionRequest",
                BatchGetUrnRequestV2.builder()
                    .urns(List.of(URN_1))
                    .aspectNames(List.of())
                    .withSystemMetadata(false)
                    .build(),
                "Bearer token");

    assertEquals(
        requestUri.get(),
        "/openapi/v3/entity/dataHubExecutionRequest/batchGet?systemMetadata=false");
    assertEquals(
        OBJECT_MAPPER.readValue(
            requestBody.get(), new TypeReference<List<Map<String, Object>>>() {}),
        List.of(Map.of("urn", URN_1)));
    assertEquals(response.getEntities().size(), 1);
    assertTrue(response.getEntities().get(0).getAspects().isEmpty());
  }
}
