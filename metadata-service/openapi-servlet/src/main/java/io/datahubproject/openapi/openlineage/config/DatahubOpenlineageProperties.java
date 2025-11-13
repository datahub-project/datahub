package io.datahubproject.openapi.openlineage.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "datahub.openlineage")
public class DatahubOpenlineageProperties {

  // Pipeline/Flow configuration
  private String pipelineName;
  private String orchestrator;
  private String env;

  // Platform configuration
  private String platformInstance;
  private String commonDatasetPlatformInstance;
  private String commonDatasetEnv;
  private String platform;

  // Dataset path configuration
  private String filePartitionRegexpPattern;

  // Metadata ingestion configuration
  private boolean materializeDataset = true;
  private boolean includeSchemaMetadata = true;
  private boolean captureColumnLevelLineage = true;

  // Advanced configuration
  private boolean usePatch = false;
}
