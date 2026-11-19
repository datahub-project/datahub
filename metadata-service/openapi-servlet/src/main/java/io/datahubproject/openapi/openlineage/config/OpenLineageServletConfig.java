package io.datahubproject.openapi.openlineage.config;

import com.linkedin.common.FabricType;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import java.util.Arrays;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    // commonDatasetEnv overrides env for the Dataset fabricType
    final boolean useCommonDatasetEnv = properties.getCommonDatasetEnv() != null;
    final FabricType fabricType =
        parseFabricType(
            useCommonDatasetEnv ? properties.getCommonDatasetEnv() : properties.getEnv(),
            useCommonDatasetEnv
                ? "datahub.openlineage.common-dataset-env"
                : "datahub.openlineage.env");

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
            .fabricType(fabricType)
            .orchestrator(properties.getOrchestrator())
            .domains(properties.getDomains())
            .parentJobUrn(null)
            .build();
    log.info("Starting OpenLineage Endpoint with config: {}", datahubOpenlineageConfig);
    return RunEventMapper.MappingConfig.builder().datahubConfig(datahubOpenlineageConfig).build();
  }

  /**
   * Fails startup on an unknown env: falling back to PROD would write every URN from this endpoint
   * into the wrong environment.
   */
  private static FabricType parseFabricType(
      @Nullable final String envValue, @Nonnull final String property) {
    if (envValue == null || envValue.isEmpty()) {
      return FabricType.PROD;
    }
    try {
      return FabricType.valueOf(envValue.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s '%s'. Valid values: %s",
              property,
              envValue,
              Arrays.stream(FabricType.values())
                  .filter(t -> t != FabricType.$UNKNOWN)
                  .map(Enum::name)
                  .collect(Collectors.joining(", "))),
          e);
    }
  }
}
