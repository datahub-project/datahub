package com.linkedin.metadata.config.usage.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

@Slf4j
public class UsageOperationsLoader {

  public static final String CLASSPATH_RESOURCE = "usage_operations.yaml";

  private final ObjectMapper yamlMapper;

  public UsageOperationsLoader(@Nonnull ObjectMapper yamlMapper) {
    this.yamlMapper = yamlMapper;
    if (yamlMapper.getPropertyNamingStrategy() == null) {
      yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }
  }

  @Nonnull
  public UsageOperationsManifest loadBundled() {
    ClassPathResource resource = new ClassPathResource(CLASSPATH_RESOURCE);
    if (!resource.exists()) {
      throw new IllegalStateException("Missing bundled usage operations manifest");
    }
    try (InputStream stream = resource.getInputStream()) {
      UsageOperationsManifest manifest =
          yamlMapper.readValue(stream, UsageOperationsManifest.class);
      validate(manifest);
      log.info(
          "Loaded usage operations manifest with {} keys", manifest.getUsageOperations().size());
      return manifest;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load usage operations manifest", e);
    }
  }

  public void validate(@Nonnull UsageOperationsManifest manifest) {
    if (manifest.getUsageOperations() == null || manifest.getUsageOperations().isEmpty()) {
      throw new IllegalStateException("usage_operations manifest must define at least one key");
    }
    for (var entry : manifest.getUsageOperations().entrySet()) {
      if (entry.getValue().getActivityClass() == null) {
        throw new IllegalStateException(
            "usage_operations." + entry.getKey() + " missing activity_class");
      }
      if (entry.getValue().getDefaultCostUnits() == null) {
        throw new IllegalStateException(
            "usage_operations." + entry.getKey() + " missing default_cost_units");
      }
      if (entry.getValue().getDefaultCostUnits() < 0) {
        throw new IllegalStateException(
            "usage_operations." + entry.getKey() + " default_cost_units must be >= 0");
      }
    }
  }
}
