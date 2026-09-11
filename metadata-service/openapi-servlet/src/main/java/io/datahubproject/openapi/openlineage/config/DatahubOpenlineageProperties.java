package io.datahubproject.openapi.openlineage.config;

import java.util.ArrayList;
import java.util.List;
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
  // Full domain URNs (urn:li:domain:<id>). OpenLineage carries no domain facet, so domains for
  // events arriving on this endpoint can only be supplied here.
  private List<String> domains = new ArrayList<>();

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
  // OpenLineage producers split a run's metadata across START, RUNNING and a terminal event.
  // Patch keeps the writes additive; a full-aspect write lets the terminal event overwrite what
  // the earlier ones declared. Job lineage is append-only as a consequence, until run-scoped
  // accumulation lands.
  private boolean usePatch = true;
}
