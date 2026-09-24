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

@Builder(toBuilder = true)
@Getter
@ToString
public class DatahubOpenlineageConfig {
  // Pipeline/Flow configuration
  @Builder.Default private final String pipelineName = null;
  private final String orchestrator;
  @Builder.Default private final FabricType fabricType = FabricType.PROD;
  // Domain URNs (urn:li:domain:<id>) attached to the emitted DataFlow and DataJob. OpenLineage has
  // no domain facet, so this is the only way to convey domain ownership for a pipeline.
  @Builder.Default private final List<String> domains = Collections.emptyList();

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

  // Microsoft Fabric OneLake: map OneLake table paths (abfss://...@onelake.dfs.fabric.microsoft.com
  // /<item>/Tables/[<schema>/]<table>) to the fabric-onelake platform using the same dataset name
  // as the fabric-onelake ingestion source (<workspaceGUID>.<itemGUID>.<schema>.<table>), instead
  // of an abs path dataset. Other OneLake paths (e.g. /Files/) stay on abs. Opt-in: enabling it
  // re-keys lineage for OneLake tables away from the abs / catalog-symlink (hive) URNs emitted
  // before, so it must not change existing users' URNs by default.
  @Builder.Default private final boolean fabricOneLakeEnabled = false;
  // Mirrors the fabric-onelake source's convert_urns_to_lowercase: lowercases schema and table,
  // and column names in column-level lineage (the source lowercases field paths too).
  // Workspace/item GUIDs are always lowercased (the Fabric REST API returns them lowercase).
  @Builder.Default private final boolean fabricOneLakeConvertUrnsToLowercase = false;
  // Mirrors the fabric-onelake source's platform_instance. Deliberately does not fall back to
  // commonDatasetPlatformInstance, which describes other sources' datasets.
  @Builder.Default private final String fabricOneLakePlatformInstance = null;
  // Friendly-name paths (<workspaceName>@.../<itemName>.<ItemType>/Tables/...) carry no GUIDs.
  // Maps "<workspaceName>/<itemName>.<ItemType>" (case-insensitive) to
  // "<workspaceGUID>/<itemGUID>" so those paths can be mapped too; unmapped ones stay on abs.
  @Builder.Default private final Map<String, String> fabricOneLakeItemIds = new HashMap<>();
  // Microsoft Fabric notebooks: key the DataFlow on the notebook item (trident.artifact.id /
  // trident.artifact.name from the spark_properties run facet) and drop the per-session prefix
  // from job names, instead of one DataFlow per Spark session (<notebook>_<session GUID>).
  // Opt-in: it renames the DataFlow / DataJob URNs of existing Fabric notebook lineage.
  @Builder.Default private final boolean fabricNotebookFlowNames = false;

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
