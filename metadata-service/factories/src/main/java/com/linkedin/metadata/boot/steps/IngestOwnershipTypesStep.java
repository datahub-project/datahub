package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.ownership.OwnershipTypeInfo;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;

/**
 * This bootstrap step is responsible for ingesting default ownership types.
 *
 * <p>If system has never bootstrapped this step will: For each ownership type defined in the yaml
 * file, it checks whether the urn exists. If not, it ingests the ownership type into DataHub.
 */
@Slf4j
@RequiredArgsConstructor
public class IngestOwnershipTypesStep implements BootstrapStep {

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private final EntityService _entityService;
  private final Resource _ownershipTypesResource;

  @Override
  public String name() {
    return "IngestOwnershipTypesStep";
  }

  @Override
  public void execute() throws Exception {
    log.info("Ingesting default ownership types from {}...", _ownershipTypesResource);

    // 1. Read from the file into JSON.
    final JsonNode ownershipTypesObj = JSON_MAPPER.readTree(_ownershipTypesResource.getFile());

    if (!ownershipTypesObj.isArray()) {
      throw new RuntimeException(
          String.format(
              "Found malformed ownership file, expected an Array but found %s",
              ownershipTypesObj.getNodeType()));
    }

    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    log.info("Ingesting {} ownership types", ownershipTypesObj.size());
    int numIngested = 0;
    for (final JsonNode roleObj : ownershipTypesObj) {
      final Urn urn = Urn.createFromString(roleObj.get("urn").asText());
      final OwnershipTypeInfo info =
          RecordUtils.toRecordTemplate(OwnershipTypeInfo.class, roleObj.get("info").toString());
      log.info(String.format("Ingesting default ownership type with urn %s", urn));
      ingestOwnershipType(urn, info, auditStamp);
      numIngested++;
    }
    log.info("Ingested {} new ownership types", numIngested);
  }

  private void ingestOwnershipType(
      final Urn ownershipTypeUrn, final OwnershipTypeInfo info, final AuditStamp auditStamp) {

    // 3. Write key & aspect MCPs.
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(ownershipTypeUrn);
    GenericAspect aspect =
        GenericRecordUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(ownershipTypeUrn, keyAspectSpec));
    keyAspectProposal.setAspect(aspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(OWNERSHIP_TYPE_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(ownershipTypeUrn);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(ownershipTypeUrn);
    proposal.setEntityType(OWNERSHIP_TYPE_ENTITY_NAME);
    proposal.setAspectName(OWNERSHIP_TYPE_INFO_ASPECT_NAME);
    info.setCreated(auditStamp);
    info.setLastModified(auditStamp);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(
        AspectsBatchImpl.builder()
            .mcps(List.of(keyAspectProposal, proposal), _entityService.getEntityRegistry())
            .build(),
        auditStamp,
        false);
  }
}
