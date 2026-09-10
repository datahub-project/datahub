package com.linkedin.metadata.config.usage.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.linkedin.metadata.config.usage.manifest.UsageMetricRegistryManifest;
import com.linkedin.metadata.config.usage.metric.MetricRegistryManifestValidation;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

@Slf4j
public class UsageMetricRegistryLoader {

  public static final String CLASSPATH_RESOURCE = "usage_metric_registry.yaml";

  private final ObjectMapper yamlMapper;

  public UsageMetricRegistryLoader(@Nonnull ObjectMapper yamlMapper) {
    this.yamlMapper = yamlMapper;
    if (yamlMapper.getPropertyNamingStrategy() == null) {
      yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }
  }

  @Nonnull
  public UsageMetricRegistryManifest loadBundled() {
    ClassPathResource resource = new ClassPathResource(CLASSPATH_RESOURCE);
    if (!resource.exists()) {
      throw new IllegalStateException("Missing bundled usage metric registry manifest");
    }
    try (InputStream stream = resource.getInputStream()) {
      UsageMetricRegistryManifest manifest =
          yamlMapper.readValue(stream, UsageMetricRegistryManifest.class);
      validate(manifest);
      log.info(
          "Loaded usage metric registry with {} metric families",
          manifest.getMetricRegistry() != null ? manifest.getMetricRegistry().size() : 0);
      return manifest;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load usage metric registry manifest", e);
    }
  }

  public void validate(@Nonnull UsageMetricRegistryManifest manifest) {
    if (manifest.getMetricRegistry() == null) {
      throw new IllegalStateException("metric_registry manifest must define at least one family");
    }
    MetricRegistryManifestValidation.validate(manifest.getMetricRegistry());
  }
}
