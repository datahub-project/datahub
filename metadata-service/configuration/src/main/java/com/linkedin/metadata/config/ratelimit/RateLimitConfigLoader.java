package com.linkedin.metadata.config.ratelimit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.PropertyResolver;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

/**
 * Merges GMS rate-limit policy after Spring binds {@code application.yaml} toggles.
 *
 * <ol>
 *   <li>Spring-bound {@link RateLimitProperties} (env placeholders already resolved)
 *   <li>Exactly one YAML document: bundled {@code rate-limit-config.yaml}, or {@code
 *       RATE_LIMITS_CONFIG_FILE} if set (replaces the classpath file; the two are not merged)
 *   <li>{@code RATE_LIMITS_CONFIG_JSON} Jackson overlay (lists replace; maps merge)
 * </ol>
 */
@Slf4j
public class RateLimitConfigLoader {

  public static final String RATE_LIMITS_CONFIG_JSON_ENV = "RATE_LIMITS_CONFIG_JSON";
  public static final String RATE_LIMITS_CONFIG_FILE_ENV = "RATE_LIMITS_CONFIG_FILE";
  private static final String CONFIG_FILE_PATH_PROPERTY = "datahub.gms.rateLimits.configFile.path";
  private static final String CONFIG_JSON_PROPERTY = "datahub.gms.rateLimits.configJson";

  private final ObjectMapper jsonMapper;
  private final ObjectMapper yamlMapper;

  public RateLimitConfigLoader(ObjectMapper jsonMapper, ObjectMapper yamlMapper) {
    this.jsonMapper = jsonMapper;
    this.yamlMapper = yamlMapper;
  }

  public RateLimitProperties loadEffective(RateLimitProperties fromSpring) {
    return loadEffective(fromSpring, null);
  }

  /**
   * Same merge as {@link #loadEffective(RateLimitProperties)}, then fills {@code configFile.path} /
   * {@code configJson} from {@code environment} when the bound bean left them unset (Hazelcast
   * {@code @Conditional} sees the {@link PropertyResolver} before {@code GMSConfiguration} exists).
   */
  public RateLimitProperties loadEffective(
      RateLimitProperties fromSpring, PropertyResolver environment) {
    RateLimitProperties effective =
        deepCopy(fromSpring != null ? fromSpring : new RateLimitProperties());
    ensureNestedDefaults(effective);
    applyOverlayPointers(effective, environment);

    applyFileOverlay(resolveConfigFilePath(effective), effective);

    String jsonOverlay = resolveJsonOverlay(effective);
    if (StringUtils.hasText(jsonOverlay)) {
      applyJsonOverlay(jsonOverlay, effective, RATE_LIMITS_CONFIG_JSON_ENV);
    }
    return effective;
  }

  void applyJsonOverlay(String json, RateLimitProperties target, String sourceLabel) {
    try {
      mergeOverlayNode(unwrapRateLimits(jsonMapper.readTree(json)), target, sourceLabel);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse rate limit overlay from " + sourceLabel, e);
    }
  }

  private void applyFileOverlay(String path, RateLimitProperties target) {
    try (InputStream stream = openConfigStream(path)) {
      mergeOverlayNode(unwrapRateLimits(yamlMapper.readTree(stream)), target, path);
    } catch (FileNotFoundException e) {
      throw new IllegalStateException("Rate limit configuration file was NOT found at " + path, e);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load rate limit configuration from: " + path, e);
    }
  }

  static String resolveConfigFilePath(RateLimitProperties config) {
    if (config.getConfigFile() != null && StringUtils.hasText(config.getConfigFile().getPath())) {
      return config.getConfigFile().getPath();
    }
    return RateLimitProperties.DEFAULT_CONFIG_FILE_PATH;
  }

  private static String resolveJsonOverlay(RateLimitProperties config) {
    if (StringUtils.hasText(config.getConfigJson())) {
      return config.getConfigJson();
    }
    return System.getenv(RATE_LIMITS_CONFIG_JSON_ENV);
  }

  private static void applyOverlayPointers(
      RateLimitProperties effective, PropertyResolver environment) {
    if (environment == null) {
      return;
    }
    String filePath =
        firstText(
            environment.getProperty(RATE_LIMITS_CONFIG_FILE_ENV),
            environment.getProperty(CONFIG_FILE_PATH_PROPERTY));
    if (StringUtils.hasText(filePath)) {
      effective.getConfigFile().setPath(filePath);
    }
    if (!StringUtils.hasText(effective.getConfigJson())) {
      String json =
          firstText(
              environment.getProperty(RATE_LIMITS_CONFIG_JSON_ENV),
              environment.getProperty(CONFIG_JSON_PROPERTY));
      if (StringUtils.hasText(json)) {
        effective.setConfigJson(json);
      }
    }
  }

  private static String firstText(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (StringUtils.hasText(value)) {
        return value;
      }
    }
    return null;
  }

  private static InputStream openConfigStream(String path) throws IOException {
    if (hasResourceScheme(path)) {
      Resource resource = new DefaultResourceLoader().getResource(path);
      if (!resource.exists()) {
        throw new FileNotFoundException(path);
      }
      log.info("Rate limit configuration found at {}", path);
      return resource.getInputStream();
    }
    ClassPathResource classpathResource = new ClassPathResource(path);
    if (classpathResource.exists()) {
      log.info("Rate limit configuration found in classpath: {}", path);
      return classpathResource.getInputStream();
    }
    FileSystemResource filesystemResource = new FileSystemResource(path);
    if (!filesystemResource.exists()) {
      throw new FileNotFoundException(path);
    }
    log.info("Rate limit configuration found in filesystem: {}", path);
    return filesystemResource.getInputStream();
  }

  private static boolean hasResourceScheme(String path) {
    return path.startsWith("file:")
        || path.startsWith("classpath:")
        || path.startsWith("http:")
        || path.startsWith("https:");
  }

  static JsonNode unwrapRateLimits(JsonNode root) {
    if (root == null || root.isNull() || root.isMissingNode()) {
      return root;
    }
    JsonNode nested = nestedRateLimits(root);
    if (nested != null) {
      return nested;
    }
    if (root.has("rateLimits")) {
      return root.get("rateLimits");
    }
    return root;
  }

  private static JsonNode nestedRateLimits(JsonNode root) {
    JsonNode datahub = root.get("datahub");
    if (datahub == null || !datahub.has("gms")) {
      return null;
    }
    JsonNode gms = datahub.get("gms");
    return gms.has("rateLimits") ? gms.get("rateLimits") : null;
  }

  private void mergeOverlayNode(JsonNode overlay, RateLimitProperties target, String sourceLabel)
      throws IOException {
    if (overlay == null || overlay.isNull() || overlay.isMissingNode()) {
      return;
    }
    ensureNestedDefaults(target);
    if (!(overlay instanceof ObjectNode)) {
      jsonMapper.readerForUpdating(target).readValue(overlay);
      log.info("Applied rate limit overlay from {}", sourceLabel);
      return;
    }
    ObjectNode remainder = ((ObjectNode) overlay).deepCopy();
    remainder.remove("configFile");
    remainder.remove("configJson");
    JsonNode heavyResolversNode = stripHeavyResolvers(remainder);
    if (!remainder.isEmpty()) {
      jsonMapper.readerForUpdating(target).readValue(remainder);
    }
    mergeHeavyResolvers(heavyResolversNode, target);
    log.info("Applied rate limit overlay from {}", sourceLabel);
  }

  private static JsonNode stripHeavyResolvers(ObjectNode remainder) {
    JsonNode scoped = remainder.get("scoped");
    if (!(scoped instanceof ObjectNode) || !scoped.has("heavyResolvers")) {
      return null;
    }
    JsonNode heavyResolvers = scoped.get("heavyResolvers");
    ((ObjectNode) scoped).remove("heavyResolvers");
    if (scoped.isEmpty()) {
      remainder.remove("scoped");
    }
    return heavyResolvers;
  }

  private void mergeHeavyResolvers(JsonNode node, RateLimitProperties target) throws IOException {
    if (node == null || node.isNull() || !node.isObject()) {
      return;
    }
    if (target.getScoped() == null) {
      target.setScoped(new RateLimitProperties.ScopedLimits());
    }
    if (target.getScoped().getHeavyResolvers() == null) {
      target.getScoped().setHeavyResolvers(new HashMap<>());
    }
    Map<String, RateLimitProperties.BucketLimits> existing = target.getScoped().getHeavyResolvers();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      RateLimitProperties.BucketLimits current = existing.get(entry.getKey());
      RateLimitProperties.BucketLimits merged =
          current != null
              ? jsonMapper.readerForUpdating(current).readValue(entry.getValue())
              : jsonMapper.treeToValue(entry.getValue(), RateLimitProperties.BucketLimits.class);
      existing.put(entry.getKey(), merged);
    }
  }

  private void ensureNestedDefaults(RateLimitProperties target) {
    if (target.getConfigFile() == null) {
      target.setConfigFile(new RateLimitProperties.ConfigFile());
    }
    if (target.getCapacity() == null) {
      target.setCapacity(new RateLimitProperties.Capacity());
    }
    if (target.getCapacity().getDefaultCapacity() == null) {
      target.getCapacity().setDefaultCapacity(new CapacityLimitConfig());
    }
    if (target.getCapacity().getGraphql() == null) {
      target.getCapacity().setGraphql(new RateLimitGraphQLConfig());
    }
    if (target.getCapacity().getRules() == null) {
      target.getCapacity().setRules(new ArrayList<>());
    }
    if (target.getEndpoint() == null) {
      target.setEndpoint(new RateLimitProperties.Endpoint());
    }
    if (target.getEndpoint().getRules() == null) {
      target.getEndpoint().setRules(new ArrayList<>());
    }
    if (target.getMetrics() == null) {
      target.setMetrics(new RateLimitProperties.Metrics());
    }
    if (target.getScoped() == null) {
      target.setScoped(new RateLimitProperties.ScopedLimits());
    }
    if (target.getScoped().getHeavyResolvers() == null) {
      target.getScoped().setHeavyResolvers(new HashMap<>());
    }
  }

  private RateLimitProperties deepCopy(RateLimitProperties source) {
    return jsonMapper.convertValue(source, RateLimitProperties.class);
  }
}
