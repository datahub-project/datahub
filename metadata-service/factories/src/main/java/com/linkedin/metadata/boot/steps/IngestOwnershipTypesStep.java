package com.linkedin.metadata.boot.steps;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.ownership.OwnershipTypeInfo;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

import static com.linkedin.metadata.Constants.*;


/**
 * This bootstrap step is responsible for ingesting default ownership types.
 * <p></p>
 * If system has never bootstrapped this step will:
 * For each ownership type defined in the yaml file, it checks whether the urn exists.
 * If not, it ingests the ownership type into DataHub.
 */
@Slf4j
@RequiredArgsConstructor
public class IngestOwnershipTypesStep implements BootstrapStep {

  private static final String UPGRADE_ID = "ingest-default-metadata-ownership-types";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private static final int SLEEP_SECONDS = 60;

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private final EntityService _entityService;

  private final EntityRegistry _entityRegistry;

  private final boolean _enableOwnershipTypeBootstrap;

  @Override
  public String name() {
    return "IngestOwnershipTypesStep";
  }

  @Override
  public void execute() throws Exception {
    // 0. Execute preflight check to see whether we need to ingest Roles
    // If ownership bootstrap is disabled, skip
    if (!_enableOwnershipTypeBootstrap) {
      log.info("{} disabled. Skipping.", this.name());
      return;
    }

    if (_entityService.exists(UPGRADE_ID_URN)) {
      log.info("Default ownership types were already ingested. Skipping ingesting again.");
      return;
    }

    log.info("Ingesting default ownership types...");

    // Sleep to ensure deployment process finishes.
    Thread.sleep(SLEEP_SECONDS * 1000);

    // 1. Read from the file into JSON.
    final JsonNode ownershipTypesObj = JSON_MAPPER.readTree(new ClassPathResource("./boot/ownership_types.json")
        .getFile());

    if (!ownershipTypesObj.isArray()) {
      throw new RuntimeException(String.format("Found malformed ownership file, expected an Array but found %s",
              ownershipTypesObj.getNodeType()));
    }

    final AspectSpec ownershipTypeInfoAspectSpec =
        _entityRegistry.getEntitySpec(OWNERSHIP_TYPE_ENTITY_NAME).getAspectSpec(OWNERSHIP_TYPE_INFO_ASPECT_NAME);
    final AuditStamp auditStamp =
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

    log.info("Ingesting {} ownership types", ownershipTypesObj.size());
    int numIngested = 0;
    for (final JsonNode roleObj : ownershipTypesObj) {
      final Urn urn = Urn.createFromString(roleObj.get("urn").asText());

      // If the info is not there, it means that the ownership type was there before, but must now be removed
      if (!roleObj.has("info")) {
        log.warn("Could not find info aspect for ownership type urn: {}. This means that the ownership type was there "
            + "before, but must now be removed.", urn);
        _entityService.deleteUrn(urn);
        continue;
      }

      final OwnershipTypeInfo info = RecordUtils.toRecordTemplate(OwnershipTypeInfo.class, roleObj.get("info")
          .toString());
      ingestOwnershipType(urn, info, auditStamp, ownershipTypeInfoAspectSpec);
      numIngested++;
    }
    log.info("Ingested {} new ownership types", numIngested);
  }

  private void ingestOwnershipType(final Urn ownershipTypeUrn, final OwnershipTypeInfo info, final AuditStamp auditStamp,
      final AspectSpec ownershipTypeInfoAspectSpec) {

    // 3. Write key & aspect
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(ownershipTypeUrn);
    GenericAspect aspect =
        GenericRecordUtils.serializeAspect(EntityKeyUtils.convertUrnToEntityKey(ownershipTypeUrn, keyAspectSpec));
    keyAspectProposal.setAspect(aspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(OWNERSHIP_TYPE_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(ownershipTypeUrn);

    _entityService.ingestProposal(keyAspectProposal, auditStamp, false);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(ownershipTypeUrn);
    proposal.setEntityType(OWNERSHIP_TYPE_ENTITY_NAME);
    proposal.setAspectName(OWNERSHIP_TYPE_INFO_ASPECT_NAME);
    info.setCreatedAt(auditStamp); // Set optional createdAt field for bootstrapped ownership types.
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(proposal, auditStamp, false);

    _entityService.produceMetadataChangeLog(ownershipTypeUrn, OWNERSHIP_TYPE_ENTITY_NAME, OWNERSHIP_TYPE_INFO_ASPECT_NAME,
        ownershipTypeInfoAspectSpec, null, info, null, null, auditStamp,
        ChangeType.RESTATE);
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }
}

