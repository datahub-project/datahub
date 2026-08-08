package com.linkedin.gms.factory.timeseries;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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
 * Proves the multi-store overlay merge model: mounted file adds {@code stores}/{@code routing}; OS
 * env (simulated as a higher-precedence property source) wins for scalars.
 */
public class PgTimeseriesConfigOverlayPrecedenceTest {

  @Test
  public void mountedFileAddsStoresAndRouting_envWinsForScalar() throws Exception {
    String overrideYaml =
        "postgres:\n"
            + "  pgTimeseries:\n"
            + "    enabled: true\n"
            + "    defaultStore: default\n"
            + "    tablePrefix: metadata_timeseries\n"
            + "    partitioning:\n"
            + "      partmanPartitionInterval: 1 day\n"
            + "      partmanPremake: 4\n"
            + "    retention:\n"
            + "      maxAgeSeconds: 7776000\n"
            + "    maintenance:\n"
            + "      cronEnabled: false\n"
            + "      intervalSeconds: 3600\n"
            + "    stores:\n"
            + "      long:\n"
            + "        tablePrefix: metadata_timeseries_long\n"
            + "        partitioning:\n"
            + "          partmanPartitionInterval: 1 month\n"
            + "          partmanPremake: 4\n"
            + "        retention:\n"
            + "          maxAgeSeconds: 46656000\n"
            + "    routing:\n"
            + "      \"[dataset.datasetprofile]\": long\n";

    StandardEnvironment env = new StandardEnvironment();
    env.getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test-env", Map.of("postgres.pgTimeseries.retention.maxAgeSeconds", "100000")));
    loadYaml("pgTimeseriesConfigOverride", overrideYaml).forEach(env.getPropertySources()::addLast);

    PostgresSqlSetupProperties props =
        Binder.get(env).bind("postgres", PostgresSqlSetupProperties.class).get();

    assertNotNull(props.getPgTimeseries().getStores().get("long"));
    assertEquals(props.getPgTimeseries().getRouting().get("dataset.datasetprofile"), "long");
    assertEquals(props.getPgTimeseries().getRetention().getMaxAgeSeconds(), 100000);
  }

  private static List<PropertySource<?>> loadYaml(String name, String yaml) throws Exception {
    return new YamlPropertySourceLoader()
        .load(name, new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8)));
  }
}
