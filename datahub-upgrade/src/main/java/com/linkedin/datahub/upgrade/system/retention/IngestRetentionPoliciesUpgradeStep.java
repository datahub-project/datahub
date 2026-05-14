package com.linkedin.datahub.upgrade.system.retention;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.key.DataHubRetentionKey;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;

/**
 * System-update non-blocking step that ingests retention policies from YAML (classpath
 * boot/retention.yaml and plugin path). Replaces the former GMS bootstrap step so retention
 * configuration runs only during system-update.
 */
@Slf4j
public class IngestRetentionPoliciesUpgradeStep implements UpgradeStep {

  private static final String STEP_ID = "IngestRetentionPolicies";
  private static final String UPGRADE_ID = "ingest-retention-policies";
  private static final String WILDCARD = "*";

  private final boolean enabled;
  private final RetentionService<?> retentionService;
  private final EntityService<?> entityService;
  private final boolean applyAfterIngest;
  private final String pluginPath;
  private final ResourcePatternResolver resolver;

  public IngestRetentionPoliciesUpgradeStep(
      final boolean enabled,
      @Nonnull final RetentionService<?> retentionService,
      @Nonnull final EntityService<?> entityService,
      final boolean applyAfterIngest,
      @Nonnull final String pluginPath,
      @Nonnull final ResourcePatternResolver resolver) {
    this.enabled = enabled;
    this.retentionService = retentionService;
    this.entityService = entityService;
    this.applyAfterIngest = applyAfterIngest;
    this.pluginPath = pluginPath;
    this.resolver = resolver;
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    if (!enabled) {
      log.debug("Retention policy ingestion is disabled; skipping step.");
      return true;
    }
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        run(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Retention policy ingestion failed", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void run(@Nonnull final OperationContext opContext) throws IOException {
    log.info("Ingesting retention policies...");

    final ObjectMapper yamlMapper = opContext.getYamlMapper();
    Resource defaultResource = resolver.getResource("classpath:boot/retention.yaml");
    Map<DataHubRetentionKey, RetentionPolicyEntry> retentionPolicyMap =
        parseYamlRetentionConfig(yamlMapper, defaultResource);

    if (!pluginPath.isEmpty()) {
      String pattern = "file:" + pluginPath + "/**/*.{yaml,yml}";
      Resource[] resources = resolver.getResources(pattern);
      for (Resource resource : resources) {
        retentionPolicyMap.putAll(parseYamlRetentionConfig(yamlMapper, resource));
      }
    }

    final Urn upgradeUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
    final boolean upgradeAlreadyApplied = entityService.exists(opContext, upgradeUrn, true);

    Map<DataHubRetentionKey, RetentionPolicyEntry> toApply = retentionPolicyMap;
    if (upgradeAlreadyApplied) {
      toApply =
          retentionPolicyMap.entrySet().stream()
              .filter(e -> e.getValue().forceOverwrite)
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
      if (toApply.isEmpty()) {
        log.info("Retention already applied and no entries have forceOverwrite. Skipping.");
        return;
      }
      log.info("Re-applying {} retention policies with forceOverwrite", toApply.size());
    }

    final EntityRegistry entityRegistry = opContext.getEntityRegistry();
    log.info("Setting {} policies", toApply.size());
    boolean hasUpdate = false;
    for (Map.Entry<DataHubRetentionKey, RetentionPolicyEntry> e : toApply.entrySet()) {
      DataHubRetentionKey key = e.getKey();
      RetentionPolicyEntry entry = e.getValue();
      if (!isValidEntityAspect(entityRegistry, key.getEntityName(), key.getAspectName())) {
        continue;
      }
      if (retentionService.setRetention(
          opContext, key.getEntityName(), key.getAspectName(), entry.config)) {
        hasUpdate = true;
      }
    }

    if (hasUpdate && applyAfterIngest) {
      log.info("Applying policies to all records");
      retentionService.batchApplyRetention(null, null);
    }

    BootstrapStep.setUpgradeResult(opContext, upgradeUrn, entityService);
  }

  private boolean isValidEntityAspect(
      final EntityRegistry registry, final String entityName, final String aspectName) {
    if (WILDCARD.equals(entityName) && WILDCARD.equals(aspectName)) {
      return true;
    }
    if (WILDCARD.equals(entityName) || WILDCARD.equals(aspectName)) {
      log.warn(
          "Retention policy with entity={}, aspect={} is invalid (wildcard must apply to both). Skipping.",
          entityName,
          aspectName);
      return false;
    }
    EntitySpec entitySpec;
    try {
      entitySpec = registry.getEntitySpec(entityName);
    } catch (IllegalArgumentException e) {
      log.warn(
          "Retention policy references unknown entity '{}'. Skipping (entity not in registry).",
          entityName);
      return false;
    }
    if (entitySpec == null) {
      log.warn(
          "Retention policy references unknown entity '{}'. Skipping (entity not in registry).",
          entityName);
      return false;
    }
    if (!entitySpec.hasAspect(aspectName)) {
      log.warn(
          "Retention policy references unknown aspect '{}' for entity '{}'. Skipping.",
          aspectName,
          entityName);
      return false;
    }
    return true;
  }

  private Map<DataHubRetentionKey, RetentionPolicyEntry> parseYamlRetentionConfig(
      final ObjectMapper yamlMapper, final Resource resource) throws IOException {
    if (!resource.exists()) {
      return Collections.emptyMap();
    }
    final JsonNode retentionPolicies = yamlMapper.readTree(resource.getInputStream());
    if (!retentionPolicies.isArray()) {
      throw new IllegalArgumentException(
          "Retention config file must contain an array of retention policies");
    }
    Map<DataHubRetentionKey, RetentionPolicyEntry> map = new LinkedHashMap<>();
    for (JsonNode node : retentionPolicies) {
      DataHubRetentionKey key = new DataHubRetentionKey();
      if (!node.has("entity")) {
        throw new IllegalArgumentException(
            "Each element in the retention config must contain field entity. Set to * for defaults");
      }
      key.setEntityName(node.get("entity").asText());
      if (!node.has("aspect")) {
        throw new IllegalArgumentException(
            "Each element in the retention config must contain field aspect. Set to * for defaults");
      }
      key.setAspectName(node.get("aspect").asText());
      if (!node.has("config")) {
        throw new IllegalArgumentException(
            "Each element in the retention config must contain field config");
      }
      DataHubRetentionConfig config =
          RecordUtils.toRecordTemplate(DataHubRetentionConfig.class, node.get("config").toString());
      boolean forceOverwrite = node.has("forceOverwrite") && node.get("forceOverwrite").asBoolean();
      map.put(key, new RetentionPolicyEntry(config, forceOverwrite));
    }
    return map;
  }

  private static final class RetentionPolicyEntry {
    final DataHubRetentionConfig config;
    final boolean forceOverwrite;

    RetentionPolicyEntry(final DataHubRetentionConfig config, final boolean forceOverwrite) {
      this.config = config;
      this.forceOverwrite = forceOverwrite;
    }
  }
}
