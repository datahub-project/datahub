package error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Environment;
import play.api.OptionalSourceMapper;
import play.api.routing.Router;
import play.http.DefaultHttpErrorHandler;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

/**
 * Custom error handler that sanitizes error responses to prevent CWE-200 (Exposure of Sensitive
 * Information to an Unauthorized Actor). Strips internal Java class names, stack traces, and source
 * location details from error messages returned to clients.
 */
@Singleton
public class SanitizedErrorHandler extends DefaultHttpErrorHandler {

  private static final Logger logger = LoggerFactory.getLogger(SanitizedErrorHandler.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Pattern JAVA_CLASS_PATTERN =
      Pattern.compile(
          "([a-z][a-z0-9_]*\\.)+[A-Z][A-Za-z0-9_]*(Exception|Error|Throwable|\\$[A-Za-z0-9_]*)");
  private static final Pattern SOURCE_LOCATION_PATTERN =
      Pattern.compile("\\[Source:.*?]|at \\[.*?]");

  private static final Map<Integer, String> SAFE_STATUS_MESSAGES =
      Map.of(
          400, "Bad Request",
          401, "Unauthorized",
          403, "Forbidden",
          404, "Not Found",
          405, "Method Not Allowed",
          408, "Request Timeout",
          413, "Request Entity Too Large",
          415, "Unsupported Media Type",
          500, "Internal Server Error",
          503, "Service Unavailable");

  @Inject
  public SanitizedErrorHandler(
      Config config,
      Environment environment,
      OptionalSourceMapper sourceMapper,
      Provider<Router> routes) {
    super(config, environment, sourceMapper, routes);
  }

  @Override
  public CompletionStage<Result> onClientError(
      Http.RequestHeader request, int statusCode, String message) {
    logger.warn(
        "Client error {}: {} {} (raw message: {})",
        statusCode,
        request.method(),
        request.uri(),
        message);

    String safeMessage = SAFE_STATUS_MESSAGES.getOrDefault(statusCode, "Request Error");
    ObjectNode body = MAPPER.createObjectNode();
    body.put("error", safeMessage);
    body.put("status", statusCode);

    return CompletableFuture.completedFuture(
        Results.status(statusCode, body).as("application/json"));
  }

  @Override
  public CompletionStage<Result> onServerError(Http.RequestHeader request, Throwable exception) {
    logger.error("Server error: {} {}", request.method(), request.uri(), exception);

    ObjectNode body = MAPPER.createObjectNode();
    body.put("error", "Internal Server Error");
    body.put("status", 500);

    return CompletableFuture.completedFuture(
        Results.internalServerError(body).as("application/json"));
  }
}
