package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entitytype.EntityTypeInfo;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Objects;
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
  public void execute() throws Exception {
    log.info("Ingesting entity types from base entity registry...");

    log.info(
        "Ingesting {} entity types", _entityService.getEntityRegistry().getEntitySpecs().size());
    int numIngested = 0;
    for (final EntitySpec spec : _entityService.getEntityRegistry().getEntitySpecs().values()) {
      final Urn entityTypeUrn =
          UrnUtils.getUrn(
              String.format("urn:li:entityType:%s.%s", DATAHUB_NAMESPACE, spec.getName()));
      final EntityTypeInfo info =
          new EntityTypeInfo()
              .setDisplayName(spec.getName()) // TODO: Support display name in the entity registry.
              .setQualifiedName(entityTypeUrn.getId());
      log.info(String.format("Ingesting entity type with urn %s", entityTypeUrn));
      ingestEntityType(entityTypeUrn, info);
      numIngested++;
    }
    log.info("Ingested {} new entity types", numIngested);
  }

  private void ingestEntityType(final Urn entityTypeUrn, final EntityTypeInfo info)
      throws Exception {
    // Write key
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(entityTypeUrn.getEntityType());
    GenericAspect keyAspect =
        GenericRecordUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(entityTypeUrn, keyAspectSpec));
    keyAspectProposal.setAspect(keyAspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(ENTITY_TYPE_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(entityTypeUrn);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(entityTypeUrn);
    proposal.setEntityType(ENTITY_TYPE_ENTITY_NAME);
    proposal.setAspectName(ENTITY_TYPE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(
        proposal,
        new AuditStamp()
            .setActor(Urn.createFromString(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()),
        false);
  }
}
