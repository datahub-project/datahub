package io.datahubproject.openapi.openlineage.config;

import com.linkedin.common.FabricType;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
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
    FabricType fabricType = FabricType.PROD; // default
    if (properties.getEnv() != null && !properties.getEnv().isEmpty()) {
      try {
        fabricType = FabricType.valueOf(properties.getEnv().toUpperCase());
      } catch (IllegalArgumentException e) {
        log.warn(
            "Invalid env value '{}'. Using default PROD. Valid values: PROD, DEV, TEST, QA, UAT, EI, PRE, STG, NON_PROD, CORP, RVW, PRD, TST, SIT, SBX, SANDBOX",
            properties.getEnv());
      }
    }

    DatahubOpenlineageConfig datahubOpenlineageConfig =
        DatahubOpenlineageConfig.builder()
            .platformInstance(properties.getPlatformInstance())
            .commonDatasetPlatformInstance(properties.getCommonDatasetPlatformInstance())
            .platform(properties.getPlatform())
            .filePartitionRegexpPattern(properties.getFilePartitionRegexpPattern())
            .materializeDataset(properties.isMaterializeDataset())
            .includeSchemaMetadata(properties.isIncludeSchemaMetadata())
            .captureColumnLevelLineage(properties.isCaptureColumnLevelLineage())
            .usePatch(properties.isUsePatch())
            .fabricType(fabricType)
            .orchestrator(properties.getOrchestrator())
            .parentJobUrn(null)
            .build();
    log.info("Starting OpenLineage Endpoint with config: {}", datahubOpenlineageConfig);
    return RunEventMapper.MappingConfig.builder().datahubConfig(datahubOpenlineageConfig).build();
  }
}
