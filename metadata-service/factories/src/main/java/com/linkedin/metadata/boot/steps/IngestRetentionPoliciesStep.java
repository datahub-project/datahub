package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.key.DataHubRetentionKey;
import com.linkedin.retention.DataHubRetentionConfig;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;

@Slf4j
@RequiredArgsConstructor
public class IngestRetentionPoliciesStep implements BootstrapStep {

  private final RetentionService<?> _retentionService;
  private final EntityService<?> _entityService;
  private final boolean _enableRetention;
  private final boolean _applyOnBootstrap;
  private final String pluginPath;
  private final ResourcePatternResolver resolver;

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    YAML_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private static final String UPGRADE_ID = "ingest-retention-policies";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  @Override
  public String name() {
    return "IngestRetentionPoliciesStep";
  }

  @Override
  public void execute(@Nonnull OperationContext systemOperationContext)
      throws IOException, URISyntaxException {
    // 0. Execute preflight check to see whether we need to ingest policies
    if (_entityService.exists(systemOperationContext, UPGRADE_ID_URN, true)) {
      log.info("Retention was applied. Skipping.");
      return;
    }
    log.info("Ingesting default retention...");

    // If retention is disabled, skip step
    if (!_enableRetention) {
      log.info("IngestRetentionPolicies disabled. Skipping.");
      return;
    }

    // 1. Read default retention config from classpath
    Resource defaultResource = resolver.getResource("classpath:boot/retention.yaml");
    Map<DataHubRetentionKey, DataHubRetentionConfig> retentionPolicyMap =
        parseYamlRetentionConfig(defaultResource);

    // 2. Read plugin retention config files from filesystem path
    if (!pluginPath.isEmpty()) {
      String pattern = "file:" + pluginPath + "/**/*.{yaml,yml}";
      Resource[] resources = resolver.getResources(pattern);
      for (Resource resource : resources) {
        retentionPolicyMap.putAll(parseYamlRetentionConfig(resource));
      }
    }

    // 4. Set the specified retention policies
    log.info("Setting {} policies", retentionPolicyMap.size());
    boolean hasUpdate = false;
    for (DataHubRetentionKey key : retentionPolicyMap.keySet()) {
      if (_retentionService.setRetention(
          systemOperationContext,
          key.getEntityName(),
          key.getAspectName(),
          retentionPolicyMap.get(key))) {
        hasUpdate = true;
      }
    }

    // 5. If there were updates on any of the retention policies, apply retention to all records
    if (hasUpdate && _applyOnBootstrap) {
      log.info("Applying policies to all records");
      _retentionService.batchApplyRetention(null, null);
    }

    BootstrapStep.setUpgradeResult(systemOperationContext, UPGRADE_ID_URN, _entityService);
  }

  /**
   * Parse yaml retention config
   *
   * <p>The structure of yaml must be a list of retention policies where each element specifies the
   * entity, aspect to apply the policy to and the policy definition. The policy definition is
   * converted into the {@link com.linkedin.retention.DataHubRetentionConfig} class.
   */
  private Map<DataHubRetentionKey, DataHubRetentionConfig> parseYamlRetentionConfig(
      Resource resource) throws IOException {
    if (!resource.exists()) {
      return Collections.emptyMap();
    }
    final JsonNode retentionPolicies = YAML_MAPPER.readTree(resource.getInputStream());
    if (!retentionPolicies.isArray()) {
      throw new IllegalArgumentException(
          "Retention config file must contain an array of retention policies");
    }

    Map<DataHubRetentionKey, DataHubRetentionConfig> retentionPolicyMap = new HashMap<>();

    for (JsonNode retentionPolicy : retentionPolicies) {
      DataHubRetentionKey key = new DataHubRetentionKey();
      if (retentionPolicy.has("entity")) {
        key.setEntityName(retentionPolicy.get("entity").asText());
      } else {
        throw new IllegalArgumentException(
            "Each element in the retention config must contain field entity. Set to * for setting defaults");
      }

      if (retentionPolicy.has("aspect")) {
        key.setAspectName(retentionPolicy.get("aspect").asText());
      } else {
        throw new IllegalArgumentException(
            "Each element in the retention config must contain field aspect. Set to * for setting defaults");
      }

      DataHubRetentionConfig retentionInfo;
      if (retentionPolicy.has("config")) {
        retentionInfo =
            RecordUtils.toRecordTemplate(
                DataHubRetentionConfig.class, retentionPolicy.get("config").toString());
      } else {
        throw new IllegalArgumentException(
            "Each element in the retention config must contain field config");
      }

      retentionPolicyMap.put(key, retentionInfo);
    }
    return retentionPolicyMap;
  }
}
