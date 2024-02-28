package io.datahubproject.openapi.openlineage.config;

import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenLineageServletConfig {

  @Bean
  public RunEventMapper.MappingConfig mappingConfig() {
    DatahubOpenlineageConfig datahubOpenlineageConfig =
        DatahubOpenlineageConfig.builder()
            .isStreaming(false)
            .pipelineName(null)
            .platformInstance(null)
            .commonDatasetPlatformInstance(null)
            .platform(null)
            .filePartitionRegexpPattern(null)
            .materializeDataset(true)
            .includeSchemaMetadata(true)
            .captureColumnLevelLineage(true)
            .usePatch(false)
            .parentJobUrn(null)
            .build();
    return RunEventMapper.MappingConfig.builder().datahubConfig(datahubOpenlineageConfig).build();
  }
}
