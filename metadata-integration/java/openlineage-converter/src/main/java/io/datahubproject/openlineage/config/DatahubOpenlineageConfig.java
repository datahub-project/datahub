package io.datahubproject.openlineage.config;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataJobUrn;
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
  @Builder.Default private final boolean isSpark = false;
  @Builder.Default private final boolean isStreaming = false;
  @Builder.Default private final String pipelineName = null;
  private final String platformInstance;
  private final String commonDatasetPlatformInstance;
  private final String platform;
  @Builder.Default private final Map<String, List<PathSpec>> pathSpecs = new HashMap<>();
  private final String filePartitionRegexpPattern;
  @Builder.Default private final FabricType fabricType = FabricType.PROD;
  private final boolean materializeDataset;
  private final boolean includeSchemaMetadata;
  @Builder.Default private final boolean captureColumnLevelLineage = true;
  @Builder.Default private final DataJobUrn parentJobUrn = null;
  // This is disabled until column level patch support won't be fixed in GMS
  @Builder.Default private final boolean usePatch = true;
  @Builder.Default private String hivePlatformAlias = "hive";
  @Builder.Default private Map<String, String> urnAliases = new HashMap<>();
  @Builder.Default private final boolean disableSymlinkResolution = false;
  @Builder.Default private final boolean lowerCaseDatasetUrns = false;
  @Builder.Default private final boolean removeLegacyLineage = false;

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
