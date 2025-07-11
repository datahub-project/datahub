package io.datahubproject.openapi.openlineage.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "datahub.openlineage")
public class DatahubOpenlineageProperties {

  private String pipelineName;
  private String platformInstance;
  private String commonDatasetPlatformInstance;
  private String platform;
  private String filePartitionRegexpPattern;
  private boolean materializeDataset = true;
  private boolean includeSchemaMetadata = true;
  private boolean captureColumnLevelLineage = true;
  private boolean usePatch = false;
}
