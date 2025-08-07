package io.datahubproject.openapi.openlineage.config;

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
            .parentJobUrn(null)
            .build();
    log.info("Starting OpenLineage Endpoint with config: {}", datahubOpenlineageConfig);
    return RunEventMapper.MappingConfig.builder().datahubConfig(datahubOpenlineageConfig).build();
  }
}
