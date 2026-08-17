package io.datahubproject.openapi.openlineage.config;

import com.linkedin.common.FabricType;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

@Configuration
@Slf4j
public class OpenLineageServletConfig {

  private final DatahubOpenlineageProperties properties;

  public OpenLineageServletConfig(DatahubOpenlineageProperties properties) {
    this.properties = properties;
  }

  @Bean
  public RunEventMapper runEventMapper() {
    return new RunEventMapper();
  }

  @Bean
  public FilterRegistrationBean<OpenLineageAuthenticationErrorFilter>
      openLineageAuthenticationErrorFilter() {
    FilterRegistrationBean<OpenLineageAuthenticationErrorFilter> registration =
        new FilterRegistrationBean<>();
    registration.setFilter(new OpenLineageAuthenticationErrorFilter());
    // Wrap authentication enforcement after credentials are extracted but before it calls
    // sendError, which bypasses controller advice.
    registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 2);
    registration.setAsyncSupported(true);
    registration.addUrlPatterns("/openapi/openlineage/*");
    return registration;
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
        fabricType = FabricType.valueOf(envValue.toUpperCase(Locale.ROOT));
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
      platformInstance = properties.getEnv().toLowerCase(Locale.ROOT);
      log.debug(
          "Using env '{}' as DataFlow cluster (platformInstance not specified)", platformInstance);
    }

    DatahubOpenlineageConfig datahubOpenlineageConfig =
        DatahubOpenlineageConfig.builder()
            .pipelineName(properties.getPipelineName())
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
            .parentJobUrn(null)
            .build();
    log.info("Starting OpenLineage Endpoint with config: {}", datahubOpenlineageConfig);
    return RunEventMapper.MappingConfig.builder().datahubConfig(datahubOpenlineageConfig).build();
  }
}
