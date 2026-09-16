package io.datahubproject.openapi.openlineage.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

  /**
   * Per-platform path-to-URN rules for object-storage datasets, keyed by platform. Without these an
   * S3 or GCS path becomes a URN built from the raw path rather than the table it represents.
   */
  private Map<String, List<PathSpecProperties>> pathSpecs = new LinkedHashMap<>();

  /**
   * Maps a connection identity to the platform instance and env that the owning platform's own
   * connector stamps, so a deployment spanning several accounts, regions or hosts emits URNs that
   * match. Keys are OpenLineage namespace authorities — {@code arn:aws:glue:{region}:{account}},
   * {@code snowflake://{account}}, {@code postgres://{host}:{port}} — and contain characters that
   * need bracket syntax in configuration, for example {@code
   * datahub.openlineage.connections[snowflake://acme].platform-instance}.
   */
  private Map<String, ConnectionProperties> connections = new LinkedHashMap<>();

  // Metadata ingestion configuration
  private boolean materializeDataset = true;
  private boolean includeSchemaMetadata = true;
  private boolean captureColumnLevelLineage = true;

  /**
   * INDIRECT upstreams are columns that influenced row selection (JOIN keys, WHERE filters, GROUP
   * BY) rather than producing the output value. Disable to keep only the columns that contributed a
   * value.
   */
  private boolean includeIndirectColumnLineage = true;

  // URN shaping
  private boolean lowerCaseDatasetUrns = false;
  private String hivePlatformAlias = "hive";
  private boolean disableSymlinkResolution = false;

  // Advanced configuration
  // OpenLineage producers split a run's metadata across START, RUNNING and a terminal event.
  // Patch keeps the writes additive; a full-aspect write lets the terminal event overwrite what
  // the earlier ones declared. Job lineage is append-only as a consequence, until run-scoped
  // accumulation lands.
  private boolean usePatch = true;

  /** Deletes the dataset-side {@code upstreamLineage} that older plugins wrote. */
  private boolean removeLegacyLineage = false;

  private boolean enhancedMergeIntoExtraction = false;

  /**
   * Caps the number of events accepted by {@code /lineage/batch}. The whole batch is converted and
   * ingested in one transaction, so an unbounded array is an unbounded amount of heap and one very
   * long-running write.
   */
  private int maxBatchSize = 1000;

  /**
   * Binding-friendly mirror of {@code io.datahubproject.openlineage.dataset.PathSpec}, which uses
   * final fields and {@code Optional} and so cannot be bound by {@code @ConfigurationProperties}
   * directly. Translated in {@link OpenLineageServletConfig}.
   */
  @Data
  public static class PathSpecProperties {
    private String alias;
    private String platform;
    private String env;
    private String platformInstance;
    private List<String> pathSpecList = new ArrayList<>();
  }

  /**
   * Binding-friendly mirror of {@code
   * io.datahubproject.openlineage.dataset.ConnectionInstanceDetail}.
   */
  @Data
  public static class ConnectionProperties {
    private String platformInstance;
    private String env;
  }
}
