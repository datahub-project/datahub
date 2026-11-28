package io.datahubproject.openapi.openlineage.config;

import com.linkedin.common.FabricType;
import com.linkedin.common.FabricTypeNames;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class OpenLineageServletConfig {

  private final DatahubOpenlineageProperties properties;

  public OpenLineageServletConfig(DatahubOpenlineageProperties properties) {
    this.properties = properties;
  }

  @Bean
  public RunEventMapper.MappingConfig mappingConfig() {
    // Parse FabricType from string property
    // Use commonDatasetEnv if specified, otherwise fall back to env
    String envValue =
        properties.getCommonDatasetEnv() != null
            ? properties.getCommonDatasetEnv()
            : properties.getEnv();

    // A FabricType value or the id of a registered customFabricType, upper-cased.
    String env = FabricType.PROD.name(); // default
    if (envValue != null && !envValue.isEmpty()) {
      Optional<String> canonical = FabricTypeNames.canonical(envValue);
      if (canonical.isPresent()) {
        env = canonical.get();
      } else {
        log.warn(
            "Invalid env value '{}'. Using default PROD. Valid values: {}, or a registered custom"
                + " environment",
            envValue,
            Arrays.toString(FabricType.values()));
      }
    }

    // Use platformInstance if specified, otherwise use env as the cluster
    String platformInstance = properties.getPlatformInstance();
    if (platformInstance == null && properties.getEnv() != null && !properties.getEnv().isEmpty()) {
      // Default: use env as the DataFlow cluster
      platformInstance = properties.getEnv().toLowerCase();
      log.debug(
          "Using env '{}' as DataFlow cluster (platformInstance not specified)", platformInstance);
    }

    DatahubOpenlineageConfig datahubOpenlineageConfig =
        DatahubOpenlineageConfig.builder()
            .platformInstance(platformInstance)
            .commonDatasetPlatformInstance(properties.getCommonDatasetPlatformInstance())
            .commonDatasetEnv(properties.getCommonDatasetEnv())
            .platform(properties.getPlatform())
            .filePartitionRegexpPattern(properties.getFilePartitionRegexpPattern())
            .materializeDataset(properties.isMaterializeDataset())
            .includeSchemaMetadata(properties.isIncludeSchemaMetadata())
            .captureColumnLevelLineage(properties.isCaptureColumnLevelLineage())
            .usePatch(properties.isUsePatch())
            .env(env)
            .orchestrator(properties.getOrchestrator())
            .domains(properties.getDomains())
            .parentJobUrn(null)
            .build();
    log.info("Starting OpenLineage Endpoint with config: {}", datahubOpenlineageConfig);
    return RunEventMapper.MappingConfig.builder().datahubConfig(datahubOpenlineageConfig).build();
  }
}
