package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entitytype.EntityTypeInfo;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** This bootstrap step is responsible for ingesting default data types. */
@Slf4j
public class IngestEntityTypesStep implements BootstrapStep {

  private static final String DATAHUB_NAMESPACE = "datahub";
  private final EntityService<?> _entityService;

  public IngestEntityTypesStep(@Nonnull final EntityService<?> entityService) {
    _entityService = Objects.requireNonNull(entityService, "entityService must not be null");
  }

  @Override
  public String name() {
    return "IngestEntityTypesStep";
  }

  @Override
  public void execute(@Nonnull OperationContext systemOperationContext) throws Exception {
    log.info("Ingesting entity types from base entity registry...");

    log.info(
        "Ingesting {} entity types",
        systemOperationContext.getEntityRegistry().getEntitySpecs().size());
    int numIngested = 0;

    Map<Urn, EntitySpec> urnEntitySpecMap =
        systemOperationContext.getEntityRegistry().getEntitySpecs().values().stream()
            .map(
                spec ->
                    Pair.of(
                        UrnUtils.getUrn(
                            String.format(
                                "urn:li:entityType:%s.%s", DATAHUB_NAMESPACE, spec.getName())),
                        spec))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    Set<Urn> existingUrns =
        _entityService.exists(systemOperationContext, urnEntitySpecMap.keySet());

    for (final Map.Entry<Urn, EntitySpec> entry : urnEntitySpecMap.entrySet()) {
      if (!existingUrns.contains(entry.getKey())) {
        final EntityTypeInfo info =
            new EntityTypeInfo()
                .setDisplayName(
                    entry
                        .getValue()
                        .getName()) // TODO: Support display name in the entity registry.
                .setQualifiedName(entry.getKey().getId());
        log.info(String.format("Ingesting entity type with urn %s", entry.getKey()));
        ingestEntityType(systemOperationContext, entry.getKey(), info);
        numIngested++;
      }
    }
    log.info("Ingested {} new entity types", numIngested);
  }

  private void ingestEntityType(
      @Nonnull OperationContext systemOperationContext,
      final Urn entityTypeUrn,
      final EntityTypeInfo info)
      throws Exception {

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(entityTypeUrn);
    proposal.setEntityType(ENTITY_TYPE_ENTITY_NAME);
    proposal.setAspectName(ENTITY_TYPE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(
        systemOperationContext,
        proposal,
        new AuditStamp()
            .setActor(Urn.createFromString(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()),
        false);
  }
}
