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

  // Microsoft Fabric OneLake: map OneLake table paths to fabric-onelake connector URNs. Opt-in,
  // because enabling it re-keys OneLake table lineage away from the abs / hive URNs used before.
  // Field names use "Onelake" (not "OneLake") so the relaxed-binding property names are
  // fabric-onelake-* (DATAHUB_OPENLINEAGE_FABRIC_ONELAKE_*), matching the platform name; the
  // converter config uses fabricOneLake*.
  private boolean fabricOnelakeEnabled = false;
  // Mirrors the fabric-onelake source's convert_urns_to_lowercase (schema, table and column names).
  private boolean fabricOnelakeConvertUrnsToLowercase = false;
  // Mirrors the fabric-onelake source's platform_instance.
  private String fabricOnelakePlatformInstance;
  // Comma-separated <workspaceName>/<itemName>.<ItemType>=<workspaceGUID>/<itemGUID> entries for
  // friendly-name OneLake paths, which carry no GUIDs.
  private String fabricOnelakeItemIds;

  // Metadata ingestion configuration
  private boolean materializeDataset = true;
  private boolean includeSchemaMetadata = true;
  private boolean captureColumnLevelLineage = true;

  // Advanced configuration
  private boolean usePatch = false;
}
