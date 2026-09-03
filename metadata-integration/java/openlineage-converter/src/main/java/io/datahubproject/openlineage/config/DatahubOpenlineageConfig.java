package io.datahubproject.openlineage.config;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataJobUrn;
import io.datahubproject.openlineage.dataset.ConnectionInstanceDetail;
import io.datahubproject.openlineage.dataset.PathSpec;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class DatahubOpenlineageConfig {
  // Pipeline/Flow configuration
  @Builder.Default private final String pipelineName = null;
  private final String orchestrator;
  @Builder.Default private final FabricType fabricType = FabricType.PROD;

  // Platform configuration
  private final String platformInstance;
  private final String commonDatasetPlatformInstance;
  private final String commonDatasetEnv;
  private final String platform;

  // Spark-specific configuration
  @Builder.Default private final boolean isSpark = false;
  @Builder.Default private final boolean isStreaming = false;

  // Dataset path configuration
  @Builder.Default private final Map<String, List<PathSpec>> pathSpecs = new HashMap<>();
  private final String filePartitionRegexpPattern;

  // Cross-platform lineage: maps a connection identity to the platform_instance/env that the
  // upstream platform's own connector stamps, so a single Spark job reading from multiple
  // accounts/regions/hosts emits matching per-connection URNs. The key is the canonical OpenLineage
  // namespace authority that identifies the connection — e.g. arn:aws:glue:{region}:{account}
  // (Glue,
  // from the symlink), snowflake://{account}, postgres://{host}:{port}. The platform is implied by
  // the namespace scheme, so no platform field is needed (mirrors PlatformDetail on the ingestion
  // side). This is also the canonical key the connection->instance registry will use.
  @Builder.Default
  private final Map<String, ConnectionInstanceDetail> connectionInstanceMap = new HashMap<>();

  // Metadata ingestion configuration
  private final boolean materializeDataset;
  private final boolean includeSchemaMetadata;
  @Builder.Default private final boolean captureColumnLevelLineage = true;
  // INDIRECT upstreams are columns that influenced row selection (JOIN keys, WHERE filters,
  // GROUP BY) rather than producing the output value. When false, input fields whose only
  // role is INDIRECT are dropped from column-level lineage.
  @Builder.Default private final boolean includeIndirectColumnLineage = true;

  // Advanced configuration
  @Builder.Default private final DataJobUrn parentJobUrn = null;
  // This is disabled until column level patch support won't be fixed in GMS
  @Builder.Default private final boolean usePatch = true;
  @Builder.Default private String hivePlatformAlias = "hive";
  @Builder.Default private Map<String, String> urnAliases = new HashMap<>();
  @Builder.Default private final boolean disableSymlinkResolution = false;
  @Builder.Default private final boolean lowerCaseDatasetUrns = false;
  @Builder.Default private final boolean removeLegacyLineage = false;
  @Builder.Default private final boolean enhancedMergeIntoExtraction = false;

  public List<PathSpec> getPathSpecsForPlatform(String platform) {
    if ((pathSpecs == null) || (pathSpecs.isEmpty())) {
      return Collections.emptyList();
    }

    return pathSpecs.values().stream()
        .filter(
            specs -> specs.stream().anyMatch(pathSpec -> pathSpec.getPlatform().equals(platform)))
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
