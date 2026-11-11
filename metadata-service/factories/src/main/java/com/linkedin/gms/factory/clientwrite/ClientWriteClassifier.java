package com.linkedin.gms.factory.clientwrite;

import com.linkedin.metadata.ratelimit.ClientClass;
import com.linkedin.metadata.ratelimit.ClientClassifier;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Decides whether a request writes metadata and, if so, which client sent it. Headers are advisory:
 * anyone can set them, so this attributes traffic for reporting and never authorizes.
 */
public final class ClientWriteClassifier {

  // Wire contracts with the Python emitter (rest_emitter.py) and the Java RestEmitter
  // (RestEmitterConfig.CLIENT_VERSION_HEADER). Keep in sync if either side renames them.
  static final String PYTHON_VERSION_HEADER = "X-DataHub-Py-Cli-Version";
  static final String JAVA_VERSION_HEADER = "X-DataHub-Java-Sdk-Version";
  static final String CLIENT_MODE_HEADER = "X-DataHub-Client-Mode";
  static final String USER_AGENT_HEADER = "User-Agent";

  static final String API_RESTLI = "restli";
  static final String API_OPENAPI = "openapi";

  static final String CLIENT_PYTHON = "python";
  static final String CLIENT_JAVA = "java";
  static final String CLIENT_BROWSER = "browser";
  static final String CLIENT_UNKNOWN = "unknown";

  static final String NONE = "none";
  static final String UNPARSEABLE = "unparseable";
  static final String OTHER = "other";

  private static final Set<String> KNOWN_CLIENT_MODES = Set.of("INGESTION", "CLI", "SDK");

  private static final Set<String> RESTLI_WRITE_RESOURCES = Set.of("/aspects", "/entities");
  private static final Set<String> RESTLI_WRITE_ACTIONS =
      Set.of("ingestProposal", "ingestProposalBatch", "ingest", "batchIngest");

  private static final List<String> OPENAPI_WRITE_PREFIXES =
      List.of(
          "/openapi/v2/entity/",
          "/openapi/v3/entity/",
          "/openapi/entities/v1/",
          "/openapi/v2/platform/entities/v1/");
  private static final Set<String> WRITE_METHODS = Set.of("POST", "PUT", "PATCH", "DELETE");
  // POST endpoints under the entity prefixes that only read.
  private static final Set<String> OPENAPI_READ_POST_SEGMENTS = Set.of("batchGet", "scroll");
  private static final String OPENAPI_V2_BATCH_GET_PREFIX = "/openapi/v2/entity/batch/";

  // Older Java SDKs never sent a version header but always set this User-Agent.
  private static final String JAVA_USER_AGENT_PREFIX = "DataHub-RestClient/";
  // "DataHub-Client/1.0 (<mode>; <component>/<caller>; <version>)" from the Python emitter.
  private static final Pattern PYTHON_USER_AGENT =
      Pattern.compile("^DataHub-Client/\\S+ \\(([^;)]*);[^;)]*;\\s*([^)]*)\\)");
  // Keeps up to four numeric segments: 1.9.0rc1 -> 1.9.0, v1.7.0.5 -> 1.7.0.5. The PEP 440 epoch
  // form of development builds (1!0.0.0.dev0) does not match and reports as unparseable.
  private static final Pattern VERSION = Pattern.compile("^v?(\\d+(?:\\.\\d+){1,3})");

  private static final int MAX_USER_AGENT_LENGTH = 160;

  private ClientWriteClassifier() {}

  /**
   * @param method HTTP method
   * @param path request path with any GMS base path already removed
   * @param queryString raw query string, or null
   * @param headers header lookup by name, returning null when absent
   * @return the write and its client, or empty when the request does not write metadata
   */
  @Nonnull
  public static Optional<ClientWrite> classify(
      @Nonnull String method,
      @Nonnull String path,
      @Nullable String queryString,
      @Nonnull UnaryOperator<String> headers) {
    String api = writeApi(method.toUpperCase(Locale.ROOT), path, queryString);
    if (api == null) {
      return Optional.empty();
    }

    String userAgent = headers.apply(USER_AGENT_HEADER);
    String client;
    String rawVersion;
    String rawMode = headers.apply(CLIENT_MODE_HEADER);
    Matcher pythonAgent = userAgent == null ? null : PYTHON_USER_AGENT.matcher(userAgent);

    String pythonVersion = headers.apply(PYTHON_VERSION_HEADER);
    String javaVersion = headers.apply(JAVA_VERSION_HEADER);
    if (pythonVersion != null) {
      client = CLIENT_PYTHON;
      rawVersion = pythonVersion;
    } else if (javaVersion != null) {
      client = CLIENT_JAVA;
      rawVersion = javaVersion;
    } else if (pythonAgent != null && pythonAgent.find()) {
      client = CLIENT_PYTHON;
      rawVersion = pythonAgent.group(2);
      rawMode = rawMode != null ? rawMode : pythonAgent.group(1);
    } else if (userAgent != null && userAgent.startsWith(JAVA_USER_AGENT_PREFIX)) {
      client = CLIENT_JAVA;
      rawVersion = userAgent.substring(JAVA_USER_AGENT_PREFIX.length());
    } else if (ClientClassifier.fromRequestSource(
            headers.apply(ClientClassifier.REQUEST_SOURCE_HEADER))
        == ClientClass.BROWSER) {
      client = CLIENT_BROWSER;
      rawVersion = null;
    } else {
      client = CLIENT_UNKNOWN;
      rawVersion = null;
    }

    return Optional.of(
        new ClientWrite(
            api,
            client,
            normalizeClientMode(rawMode),
            normalizeVersion(rawVersion),
            sanitizeForLog(userAgent, MAX_USER_AGENT_LENGTH)));
  }

  @Nullable
  private static String writeApi(
      @Nonnull String method, @Nonnull String path, @Nullable String queryString) {
    if (!WRITE_METHODS.contains(method)) {
      return null;
    }
    if ("POST".equals(method) && RESTLI_WRITE_RESOURCES.contains(path)) {
      String action = queryParam(queryString, "action");
      return action != null && RESTLI_WRITE_ACTIONS.contains(action) ? API_RESTLI : null;
    }
    String withSlash = path.endsWith("/") ? path : path + "/";
    boolean entityPath = OPENAPI_WRITE_PREFIXES.stream().anyMatch(withSlash::startsWith);
    if (!entityPath) {
      return null;
    }
    if ("POST".equals(method)
        && (path.startsWith(OPENAPI_V2_BATCH_GET_PREFIX)
            || OPENAPI_READ_POST_SEGMENTS.contains(lastSegment(path)))) {
      return null;
    }
    return API_OPENAPI;
  }

  @Nonnull
  static String normalizeVersion(@Nullable String raw) {
    if (raw == null || raw.isBlank()) {
      return NONE;
    }
    Matcher matcher = VERSION.matcher(raw.trim());
    return matcher.find() ? matcher.group(1) : UNPARSEABLE;
  }

  @Nonnull
  static String normalizeClientMode(@Nullable String raw) {
    if (raw == null || raw.isBlank()) {
      return NONE;
    }
    String mode = raw.trim().toUpperCase(Locale.ROOT);
    return KNOWN_CLIENT_MODES.contains(mode) ? mode : OTHER;
  }

  /** Printable ASCII only, double quotes swapped out, truncated: safe inside a quoted log field. */
  @Nonnull
  static String sanitizeForLog(@Nullable String value, int maxLength) {
    if (value == null || value.isEmpty()) {
      return "";
    }
    StringBuilder out = new StringBuilder(Math.min(value.length(), maxLength));
    for (int i = 0; i < value.length() && out.length() < maxLength; i++) {
      char c = value.charAt(i);
      if (c == '"') {
        out.append('\'');
      } else if (c >= 0x20 && c <= 0x7E) {
        out.append(c);
      } else {
        out.append('?');
      }
    }
    return out.toString();
  }

  @Nullable
  private static String queryParam(@Nullable String queryString, @Nonnull String name) {
    if (queryString == null || queryString.isEmpty()) {
      return null;
    }
    for (String pair : queryString.split("&")) {
      int eq = pair.indexOf('=');
      String key = eq < 0 ? pair : pair.substring(0, eq);
      if (name.equals(key)) {
        String value = eq < 0 ? "" : pair.substring(eq + 1);
        try {
          return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
          return value;
        }
      }
    }
    return null;
  }

  @Nonnull
  private static String lastSegment(@Nonnull String path) {
    String trimmed = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    return trimmed.substring(trimmed.lastIndexOf('/') + 1);
  }
}
