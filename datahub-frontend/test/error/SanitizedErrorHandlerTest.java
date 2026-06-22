package error;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import play.api.OptionalSourceMapper;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;
import scala.Option;

public class SanitizedErrorHandlerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private SanitizedErrorHandler handler;

  @BeforeEach
  void setUp() {
    handler =
        new SanitizedErrorHandler(
            com.typesafe.config.ConfigFactory.load(),
            play.Environment.simple(),
            new OptionalSourceMapper(Option.empty()),
            () -> null);
  }

  @Test
  void onClientError_400_returnsJsonWithSafeMessage() throws Exception {
    Http.RequestHeader request = Helpers.fakeRequest("POST", "/api/v2/graphql").build();

    Result result = completeResult(handler.onClientError(request, 400, "raw details"));

    assertEquals(400, result.status());
    assertEquals("application/json", result.contentType().orElse(""));

    JsonNode body = parseBody(result);
    assertEquals("Bad Request", body.get("error").asText());
    assertEquals(400, body.get("status").asInt());
  }

  @Test
  void onClientError_doesNotLeakJacksonException() throws Exception {
    String leakyMessage =
        "Error decoding json body: com.fasterxml.jackson.core.JsonParseException: "
            + "Unexpected character (']') (code 93): expected a valid value "
            + "at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); "
            + "line: 1, column: 18]";
    Http.RequestHeader request = Helpers.fakeRequest("POST", "/api/v2/graphql").build();

    Result result = completeResult(handler.onClientError(request, 400, leakyMessage));

    assertEquals(400, result.status());
    String responseBody = Helpers.contentAsString(result);
    assertFalse(responseBody.contains("com.fasterxml.jackson"), "Must not contain class names");
    assertFalse(responseBody.contains("StreamReadFeature"), "Must not contain internal config");
    assertFalse(responseBody.contains("[Source:"), "Must not contain source location");

    JsonNode body = MAPPER.readTree(responseBody);
    assertEquals("Bad Request", body.get("error").asText());
  }

  @Test
  void onClientError_knownStatusCodes_returnCorrectMessage() throws Exception {
    int[] knownCodes = {400, 401, 403, 404, 405, 408, 413, 415, 500, 503};
    for (int statusCode : knownCodes) {
      Http.RequestHeader request = Helpers.fakeRequest("GET", "/test").build();

      Result result = completeResult(handler.onClientError(request, statusCode, "internal detail"));

      assertEquals(statusCode, result.status(), "Status code mismatch for " + statusCode);
      JsonNode body = parseBody(result);
      assertNotNull(body.get("error").asText());
      assertFalse(
          body.get("error").asText().contains("internal detail"),
          "Status " + statusCode + " must not leak raw message");
      assertEquals(statusCode, body.get("status").asInt());
    }
  }

  @Test
  void onClientError_unknownStatusCode_returnsGenericMessage() throws Exception {
    Http.RequestHeader request = Helpers.fakeRequest("GET", "/test").build();

    Result result = completeResult(handler.onClientError(request, 422, "validation failed"));

    assertEquals(422, result.status());
    JsonNode body = parseBody(result);
    assertEquals("Request Error", body.get("error").asText());
  }

  @Test
  void onServerError_returnsGenericJsonError() throws Exception {
    Http.RequestHeader request = Helpers.fakeRequest("GET", "/api/graphql").build();
    RuntimeException exception =
        new RuntimeException("com.fasterxml.jackson.core.JsonParseException: bad");

    Result result = completeResult(handler.onServerError(request, exception));

    assertEquals(500, result.status());
    assertEquals("application/json", result.contentType().orElse(""));

    String responseBody = Helpers.contentAsString(result);
    assertFalse(responseBody.contains("com.fasterxml.jackson"), "Must not leak class names");
    assertFalse(responseBody.contains("JsonParseException"), "Must not leak exception types");

    JsonNode body = MAPPER.readTree(responseBody);
    assertEquals("Internal Server Error", body.get("error").asText());
    assertEquals(500, body.get("status").asInt());
  }

  @Test
  void onServerError_doesNotLeakStackTrace() throws Exception {
    Http.RequestHeader request = Helpers.fakeRequest("POST", "/api/v2/graphql").build();
    Exception exception = new NullPointerException("secret internal state");

    Result result = completeResult(handler.onServerError(request, exception));

    String responseBody = Helpers.contentAsString(result);
    assertFalse(responseBody.contains("NullPointerException"));
    assertFalse(responseBody.contains("secret internal state"));
    assertFalse(responseBody.contains("at "));
  }

  @Test
  void responseIsAlwaysJson() throws Exception {
    Http.RequestHeader request = Helpers.fakeRequest("GET", "/missing").build();

    Result clientResult = completeResult(handler.onClientError(request, 404, "not found"));
    Result serverResult =
        completeResult(handler.onServerError(request, new RuntimeException("boom")));

    assertEquals("application/json", clientResult.contentType().orElse(""));
    assertEquals("application/json", serverResult.contentType().orElse(""));
  }

  private static Result completeResult(CompletionStage<Result> stage) throws Exception {
    return stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private static JsonNode parseBody(Result result) throws Exception {
    return MAPPER.readTree(Helpers.contentAsString(result));
  }
}
