package com.linkedin.datahub.upgrade.system.ingestion;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entitytype.EntityTypeInfo;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** System-update step that ingests EntityTypeInfo aspects for all entity types in registry. */
@Slf4j
public class IngestEntityTypesStep implements UpgradeStep {

  static final String UPGRADE_ID = "ingest-entity-types-v1";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
  private static final String DATAHUB_NAMESPACE = "datahub";

  private final EntityService<?> entityService;
  private final boolean enabled;

  public IngestEntityTypesStep(
      @Nonnull final EntityService<?> entityService, final boolean enabled) {
    this.entityService = entityService;
    this.enabled = enabled;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    if (!enabled) {
      log.info("IngestEntityTypes is disabled. Skipping.");
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
        log.error("Entity type ingestion failed", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void run(@Nonnull final OperationContext opContext) {
    log.info("Ingesting entity types from entity registry...");

    final Map<Urn, EntitySpec> urnEntitySpecMap =
        opContext.getEntityRegistry().getEntitySpecs().values().stream()
            .map(
                spec ->
                    Pair.of(
                        UrnUtils.getUrn(
                            String.format(
                                "urn:li:entityType:%s.%s", DATAHUB_NAMESPACE, spec.getName())),
                        spec))
            .collect(
                Collectors.toMap(
                    Pair::getKey,
                    Pair::getValue,
                    (existing, duplicate) -> {
                      log.warn(
                          "Duplicate entity type URN detected for entity '{}', keeping first",
                          existing.getName());
                      return existing;
                    }));

    final Set<Urn> existingUrns = entityService.exists(opContext, urnEntitySpecMap.keySet());

    log.info("Ingesting {} entity types", urnEntitySpecMap.size());
    int numIngested = 0;
    for (final Map.Entry<Urn, EntitySpec> entry : urnEntitySpecMap.entrySet()) {
      if (!existingUrns.contains(entry.getKey())) {
        final EntityTypeInfo info =
            new EntityTypeInfo()
                .setQualifiedName(entry.getKey().getId())
                .setDisplayName(entry.getValue().getName());
        ingestEntityType(opContext, entry.getKey(), info);
        numIngested++;
      }
    }

    BootstrapStep.setUpgradeResult(opContext, UPGRADE_ID_URN, entityService);
    log.info("Ingested {} new entity types", numIngested);
  }

  private void ingestEntityType(
      @Nonnull final OperationContext opContext,
      final Urn entityTypeUrn,
      final EntityTypeInfo info) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(entityTypeUrn);
    proposal.setEntityType(ENTITY_TYPE_ENTITY_NAME);
    proposal.setAspectName(ENTITY_TYPE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    entityService.ingestProposal(
        opContext,
        proposal,
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()),
        false);
  }
}
