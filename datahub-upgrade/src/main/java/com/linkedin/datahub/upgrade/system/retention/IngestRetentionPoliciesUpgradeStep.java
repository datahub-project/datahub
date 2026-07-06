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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
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
  private final boolean applyOnPolicyChange;
  private final boolean overwriteNonSystemPolicies;
  private final String pluginPath;
  private final ResourcePatternResolver resolver;

  public IngestRetentionPoliciesUpgradeStep(
      final boolean enabled,
      @Nonnull final RetentionService<?> retentionService,
      @Nonnull final EntityService<?> entityService,
      final boolean applyAfterIngest,
      final boolean applyOnPolicyChange,
      final boolean overwriteNonSystemPolicies,
      @Nonnull final String pluginPath,
      @Nonnull final ResourcePatternResolver resolver) {
    this.enabled = enabled;
    this.retentionService = retentionService;
    this.entityService = entityService;
    this.applyAfterIngest = applyAfterIngest;
    this.applyOnPolicyChange = applyOnPolicyChange;
    this.overwriteNonSystemPolicies = overwriteNonSystemPolicies;
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

    final EntityRegistry entityRegistry = opContext.getEntityRegistry();
    boolean hasUpdate = false;
    int updatedCount = 0;
    boolean needsGlobalBatch = false;
    final List<DataHubRetentionKey> specificBatchScopes = new ArrayList<>();
    int skippedUnchanged = 0;
    int skippedDrift = 0;

    for (Map.Entry<DataHubRetentionKey, RetentionPolicyEntry> e : retentionPolicyMap.entrySet()) {
      DataHubRetentionKey key = e.getKey();
      RetentionPolicyEntry entry = e.getValue();
      if (!isValidEntityAspect(entityRegistry, key.getEntityName(), key.getAspectName())) {
        continue;
      }

      final String entityName = key.getEntityName();
      final String aspectName = key.getAspectName();
      final DataHubRetentionConfig desiredConfig = entry.config;

      Optional<DataHubRetentionConfig> storedConfig =
          retentionService.getRetentionConfigAtExactKey(opContext, entityName, aspectName);
      if (storedConfig.isPresent()
          && retentionService.retentionConfigEquals(storedConfig.get(), desiredConfig)) {
        skippedUnchanged++;
        continue;
      }

      final boolean mayOverwrite =
          !upgradeAlreadyApplied
              || storedConfig.isEmpty()
              || retentionService.isRetentionPolicySystemManaged(opContext, entityName, aspectName)
              || overwriteNonSystemPolicies
              || entry.forceOverwrite;

      if (!mayOverwrite) {
        skippedDrift++;
        final Optional<Urn> lastWriter =
            retentionService.getRetentionPolicyLastWriter(opContext, entityName, aspectName);
        log.warn(
            "Retention policy for entity={}, aspect={} differs from desired config but was not "
                + "updated because the stored policy was not system-managed (last writer: {}). "
                + "Set ENTITY_SERVICE_RETENTION_OVERWRITE_NON_SYSTEM_POLICIES=true, add "
                + "forceOverwrite: true for this entry, or update the policy via API.",
            entityName,
            aspectName,
            lastWriter.map(Urn::toString).orElse("unknown"));
        continue;
      }

      if (retentionService.setRetention(opContext, entityName, aspectName, desiredConfig)) {
        hasUpdate = true;
        updatedCount++;
        if (WILDCARD.equals(entityName) && WILDCARD.equals(aspectName)) {
          needsGlobalBatch = true;
        } else if (!needsGlobalBatch) {
          specificBatchScopes.add(key);
        }
      }
    }

    if (skippedDrift > 0) {
      log.warn(
          "{} retention {} differ from desired config but were not updated (not system-managed). "
              + "See warnings above for remediation.",
          skippedDrift,
          skippedDrift == 1 ? "policy" : "policies");
    }

    log.info(
        "Retention ingest complete: {} updated, {} unchanged, {} skipped due to drift",
        updatedCount,
        skippedUnchanged,
        skippedDrift);

    if (applyAfterIngest && !hasUpdate) {
      log.info("Applying retention policies to all records (applyOnBootstrap)");
      retentionService.batchApplyRetention(null, null);
    } else if (hasUpdate && (applyOnPolicyChange || applyAfterIngest)) {
      if (applyAfterIngest || needsGlobalBatch) {
        log.info("Applying retention policies to all records");
        retentionService.batchApplyRetention(null, null);
      } else {
        for (DataHubRetentionKey scope : specificBatchScopes) {
          log.info(
              "Applying retention policies for entity={}, aspect={}",
              scope.getEntityName(),
              scope.getAspectName());
          retentionService.batchApplyRetention(scope.getEntityName(), scope.getAspectName());
        }
      }
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
