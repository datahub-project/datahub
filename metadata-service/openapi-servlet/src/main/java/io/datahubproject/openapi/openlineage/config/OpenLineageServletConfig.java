package io.datahubproject.openapi.openlineage.config;

import com.linkedin.common.FabricType;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.ConnectionInstanceDetail;
import io.datahubproject.openlineage.dataset.PathSpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
    // Parse FabricType from string property
    // Use commonDatasetEnv if specified, otherwise fall back to env
    String envValue =
        properties.getCommonDatasetEnv() != null
            ? properties.getCommonDatasetEnv()
            : properties.getEnv();

    FabricType fabricType = FabricType.PROD; // default
    if (envValue != null && !envValue.isEmpty()) {
      try {
        fabricType = FabricType.valueOf(envValue.toUpperCase());
      } catch (IllegalArgumentException e) {
        log.warn(
            "Invalid env value '{}'. Using default PROD. Valid values: PROD, DEV, TEST, QA, UAT, EI, PRE, STG, NON_PROD, CORP, RVW, PRD, TST, SIT, SBX, SANDBOX, CERT",
            envValue);
      }
    }

    // Use platformInstance if specified, otherwise use env as the cluster
    String platformInstance = properties.getPlatformInstance();
    if (platformInstance == null && properties.getEnv() != null && !properties.getEnv().isEmpty()) {
      // Default: use env as the DataFlow cluster
      platformInstance = properties.getEnv().toLowerCase();
      log.debug(
          "Using env '{}' as DataFlow cluster (platformInstance not specified)", platformInstance);
    }

    DatahubOpenlineageConfig datahubOpenlineageConfig =
        DatahubOpenlineageConfig.builder()
            .platformInstance(platformInstance)
            .commonDatasetPlatformInstance(properties.getCommonDatasetPlatformInstance())
            .commonDatasetEnv(properties.getCommonDatasetEnv())
            .platform(properties.getPlatform())
            .filePartitionRegexpPattern(properties.getFilePartitionRegexpPattern())
            .materializeDataset(properties.isMaterializeDataset())
            .includeSchemaMetadata(properties.isIncludeSchemaMetadata())
            .captureColumnLevelLineage(properties.isCaptureColumnLevelLineage())
            .usePatch(properties.isUsePatch())
            .fabricType(fabricType)
            .orchestrator(properties.getOrchestrator())
            .domains(properties.getDomains())
            .pipelineName(properties.getPipelineName())
            .pathSpecs(toPathSpecs(properties.getPathSpecs()))
            .connectionInstanceMap(toConnectionInstanceMap(properties.getConnections()))
            .lowerCaseDatasetUrns(properties.isLowerCaseDatasetUrns())
            .hivePlatformAlias(properties.getHivePlatformAlias())
            .disableSymlinkResolution(properties.isDisableSymlinkResolution())
            .removeLegacyLineage(properties.isRemoveLegacyLineage())
            .includeIndirectColumnLineage(properties.isIncludeIndirectColumnLineage())
            .enhancedMergeIntoExtraction(properties.isEnhancedMergeIntoExtraction())
            .parentJobUrn(null)
            .build();
    log.info("Starting OpenLineage Endpoint with config: {}", datahubOpenlineageConfig);
    return RunEventMapper.MappingConfig.builder().datahubConfig(datahubOpenlineageConfig).build();
  }

  /**
   * Translates the bound path-spec properties into the converter's {@code PathSpec}, which uses
   * final fields and {@code Optional} and so cannot be bound directly.
   */
  private static Map<String, List<PathSpec>> toPathSpecs(
      Map<String, List<DatahubOpenlineageProperties.PathSpecProperties>> configured) {
    Map<String, List<PathSpec>> pathSpecs = new HashMap<>();
    if (configured == null) {
      return pathSpecs;
    }
    configured.forEach(
        (platform, specs) ->
            pathSpecs.put(
                platform,
                specs.stream()
                    .map(
                        spec ->
                            PathSpec.builder()
                                .alias(spec.getAlias())
                                .platform(
                                    spec.getPlatform() == null ? platform : spec.getPlatform())
                                .pathSpecList(spec.getPathSpecList())
                                .env(Optional.ofNullable(spec.getEnv()))
                                .platformInstance(Optional.ofNullable(spec.getPlatformInstance()))
                                .build())
                    .collect(Collectors.toList())));
    return pathSpecs;
  }

  /**
   * Translates the bound connection properties into {@code ConnectionInstanceDetail}. {@code env}
   * is parsed to a {@link FabricType} here so an invalid value is reported once, at startup, rather
   * than being dropped silently for every dataset during URN construction.
   */
  private static Map<String, ConnectionInstanceDetail> toConnectionInstanceMap(
      Map<String, DatahubOpenlineageProperties.ConnectionProperties> configured) {
    Map<String, ConnectionInstanceDetail> connections = new HashMap<>();
    if (configured == null) {
      return connections;
    }
    configured.forEach(
        (key, connection) -> {
          Optional<FabricType> env = Optional.empty();
          if (connection.getEnv() != null && !connection.getEnv().isEmpty()) {
            try {
              env = Optional.of(FabricType.valueOf(connection.getEnv().toUpperCase()));
            } catch (IllegalArgumentException e) {
              log.warn(
                  "Invalid env '{}' for OpenLineage connection '{}'; falling back to the global env.",
                  connection.getEnv(),
                  key);
            }
          }
          connections.put(
              key,
              ConnectionInstanceDetail.builder()
                  .platformInstance(Optional.ofNullable(connection.getPlatformInstance()))
                  .env(env)
                  .build());
        });
    return connections;
  }
}
