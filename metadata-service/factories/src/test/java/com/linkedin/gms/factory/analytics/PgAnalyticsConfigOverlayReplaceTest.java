package com.linkedin.gms.factory.analytics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ByteArrayResource;
import org.testng.annotations.Test;

/**
 * Proves {@code DATAHUB_PGANALYTICS_CONFIG_FILE} document-replace semantics: application.yaml
 * leaves stores/routing empty, and the active config document fully owns those maps (no merge with
 * a prior product default).
 */
public class PgAnalyticsConfigOverlayReplaceTest {

  private static final String APPLICATION_BASE =
      "postgres:\n"
          + "  pgAnalytics:\n"
          + "    enabled: true\n"
          + "    defaultStore: default\n"
          + "    tablePrefix: metadata_analytics\n"
          + "    stores: {}\n"
          + "    routing: {}\n";

  private static final String BUNDLED_DEFAULTS =
      "postgres:\n"
          + "  pgAnalytics:\n"
          + "    stores:\n"
          + "      product:\n"
          + "        tablePrefix: metadata_analytics_product\n"
          + "    routing:\n"
          + "      datahub_usage: product\n";

  @Test
  public void bundledDefaultsPopulateProductStore() throws Exception {
    StandardEnvironment env = new StandardEnvironment();
    loadYaml("application", APPLICATION_BASE).forEach(env.getPropertySources()::addLast);
    loadYaml("pgAnalyticsConfigOverride", BUNDLED_DEFAULTS)
        .forEach(env.getPropertySources()::addFirst);

    PostgresSqlSetupProperties props =
        Binder.get(env).bind("postgres", PostgresSqlSetupProperties.class).get();

    assertNotNull(props.getPgAnalytics().getStores().get("product"));
    assertEquals(props.getPgAnalytics().getRouting().get("datahub_usage"), "product");
  }

  @Test
  public void mountedFileWithoutProductFullyReplacesBundledDefaults() throws Exception {
    String mounted =
        "postgres:\n"
            + "  pgAnalytics:\n"
            + "    stores:\n"
            + "      other:\n"
            + "        tablePrefix: metadata_analytics_other\n"
            + "    routing:\n"
            + "      api_usage: other\n";

    StandardEnvironment env = new StandardEnvironment();
    loadYaml("application", APPLICATION_BASE).forEach(env.getPropertySources()::addLast);
    // Simulate DATAHUB_PGANALYTICS_CONFIG_FILE swap: only the mounted file is present (bundled
    // classpath document is not also loaded).
    loadYaml("pgAnalyticsConfigOverride", mounted).forEach(env.getPropertySources()::addFirst);

    PostgresSqlSetupProperties props =
        Binder.get(env).bind("postgres", PostgresSqlSetupProperties.class).get();

    assertFalse(props.getPgAnalytics().getStores().containsKey("product"));
    assertTrue(props.getPgAnalytics().getStores().containsKey("other"));
    assertFalse(props.getPgAnalytics().getRouting().containsKey("datahub_usage"));
    assertEquals(props.getPgAnalytics().getRouting().get("api_usage"), "other");
  }

  @Test
  public void envScalarStillOverridesMountedTablePrefix() throws Exception {
    String mounted =
        "postgres:\n"
            + "  pgAnalytics:\n"
            + "    stores:\n"
            + "      product:\n"
            + "        tablePrefix: metadata_analytics_product\n"
            + "    routing:\n"
            + "      datahub_usage: product\n";

    StandardEnvironment env = new StandardEnvironment();
    loadYaml("application", APPLICATION_BASE).forEach(env.getPropertySources()::addLast);
    loadYaml("pgAnalyticsConfigOverride", mounted).forEach(env.getPropertySources()::addFirst);
    // Env / system properties win over the mounted document (add after YAML so this is first).
    env.getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test-env",
                Map.of(
                    "postgres.pgAnalytics.stores.product.tablePrefix",
                    "metadata_analytics_product_custom")));

    PostgresSqlSetupProperties props =
        Binder.get(env).bind("postgres", PostgresSqlSetupProperties.class).get();

    assertEquals(
        props.getPgAnalytics().getStores().get("product").getTablePrefix(),
        "metadata_analytics_product_custom");
  }

  private static List<PropertySource<?>> loadYaml(String name, String yaml) throws Exception {
    return new YamlPropertySourceLoader()
        .load(name, new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8)));
  }
}
