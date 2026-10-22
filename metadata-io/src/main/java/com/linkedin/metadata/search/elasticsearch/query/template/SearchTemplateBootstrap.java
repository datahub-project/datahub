package com.linkedin.metadata.search.elasticsearch.query.template;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.search.template.SearchTemplateProperties;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

/**
 * Shipped search templates from the bootstrap MCP file. System update always reads this catalog.
 * GMS uses it only when the aspect has not been stored.
 */
@Slf4j
public final class SearchTemplateBootstrap {
  static final String INDEX = "bootstrap_mcps.yaml";
  static final String ENTRY = "search-templates";
  private static final String ASPECT = "searchTemplateProperties";
  // This pass only consumes delimiter switches. Interpolated values are JSON and must be inserted
  // unchanged. The default factory HTML-encodes them.
  private static final MustacheFactory MUSTACHE =
      new DefaultMustacheFactory() {
        @Override
        public void encode(String value, Writer writer) {
          try {
            writer.write(value);
          } catch (IOException e) {
            throw new IllegalStateException("Could not render shipped search templates", e);
          }
        }
      };
  private static final TypeReference<List<JsonNode>> ROWS = new TypeReference<>() {};
  private static final Set<String> LOGGED = ConcurrentHashMap.newKeySet();

  private SearchTemplateBootstrap() {}

  /** Loads the shipped catalog on first use without a shared monitor. */
  private static final class CatalogHolder {
    private static final Map<String, SearchTemplateProperties> CATALOG = load();

    private CatalogHolder() {}
  }

  @Nonnull
  public static Set<String> urns() {
    return Set.copyOf(catalog().keySet());
  }

  @Nullable
  public static SearchTemplateProperties get(@Nonnull String urn) {
    SearchTemplateProperties properties = catalog().get(urn);
    return properties == null ? null : copy(properties);
  }

  public static void note(@Nonnull String urn) {
    if (LOGGED.add(urn)) {
      log.info("Search template {} is being read from the shipped bootstrap file", urn);
    }
  }

  @Nonnull
  private static Map<String, SearchTemplateProperties> catalog() {
    return CatalogHolder.CATALOG;
  }

  @Nonnull
  private static Map<String, SearchTemplateProperties> load() {
    String location = location();
    if (location == null) {
      log.info("No shipped search templates at {}", INDEX);
      return Map.of();
    }
    String raw = read(location);
    if (raw == null) {
      log.info("Shipped search templates were not found at {}", location);
      return Map.of();
    }
    String rendered = render(raw, location);
    List<JsonNode> rows;
    try {
      rows = SearchTemplateMappers.YAML.readValue(rendered, ROWS);
    } catch (IOException e) {
      throw new IllegalStateException("Could not parse shipped search templates at " + location, e);
    }
    Map<String, SearchTemplateProperties> byUrn = new LinkedHashMap<>();
    for (JsonNode row : rows) {
      if (!ASPECT.equals(row.path("aspectName").asText())) {
        continue;
      }
      String urn = row.path("entityUrn").asText();
      try {
        String aspect = SearchTemplateMappers.JSON.writeValueAsString(row.get("aspect"));
        byUrn.put(urn, RecordUtils.toRecordTemplate(SearchTemplateProperties.class, aspect));
      } catch (IOException e) {
        throw new IllegalStateException("Could not read shipped search template " + urn, e);
      }
    }
    log.info("Loaded {} shipped search templates from {}", byUrn.size(), location);
    return Collections.unmodifiableMap(byUrn);
  }

  @Nullable
  private static String location() {
    JsonNode index = readTree(INDEX);
    if (index == null) {
      return null;
    }
    for (JsonNode entry : index.path("bootstrap").path("templates")) {
      if (ENTRY.equals(entry.path("name").asText())) {
        String location = entry.path("mcps_location").asText(null);
        return location == null || location.isBlank() ? null : location;
      }
    }
    return null;
  }

  @Nonnull
  static String render(@Nonnull String raw, @Nonnull String location) {
    Map<String, Object> scope = new HashMap<>();
    scope.put("auditStamp", RecordUtils.toJsonString(AuditStampUtils.createDefaultAuditStamp()));
    StringWriter writer = new StringWriter();
    try {
      Mustache mustache = MUSTACHE.compile(new StringReader(raw), location);
      mustache.execute(writer, scope).flush();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Could not render shipped search templates at " + location, e);
    }
    return writer.toString();
  }

  @Nullable
  private static String read(@Nonnull String location) {
    try (InputStream in = new ClassPathResource(location).getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return null;
    }
  }

  @Nullable
  private static JsonNode readTree(@Nonnull String location) {
    try (InputStream in = new ClassPathResource(location).getInputStream()) {
      return SearchTemplateMappers.YAML.readTree(in);
    } catch (IOException e) {
      return null;
    }
  }

  @Nonnull
  private static SearchTemplateProperties copy(@Nonnull SearchTemplateProperties properties) {
    try {
      return properties.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }
}
