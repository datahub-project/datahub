package com.linkedin.metadata.boot.steps;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.ownership.OwnershipTypeInfo;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

import java.util.List;

import static com.linkedin.metadata.Constants.*;


/**
 * This bootstrap step is responsible for ingesting default ownership types.
 * <p></p>
 * If system has never bootstrapped this step will:
 * For each ownership type defined in the yaml file, it checks whether the urn exists.
 * If not, it ingests the ownership type into DataHub.
 */
@Slf4j
public class IngestOwnershipTypesStep extends UpgradeStep {

  private static final String UPGRADE_ID = "ingest-default-metadata-ownership-types";
  private static final String VERSION = "1";
  private static final int SLEEP_SECONDS = 60;

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  public IngestOwnershipTypesStep(EntityService entityService) {
    super(entityService, VERSION, UPGRADE_ID);
  }

  @Override
  public String name() {
    return "IngestOwnershipTypesStep";
  }

  @Override
  public void upgrade() throws Exception {
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

    final AspectSpec ownershipTypeInfoAspectSpec = _entityService.getEntityRegistry()
        .getEntitySpec(OWNERSHIP_TYPE_ENTITY_NAME)
        .getAspectSpec(OWNERSHIP_TYPE_INFO_ASPECT_NAME);
    final AuditStamp auditStamp =
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

    log.info("Ingesting {} ownership types", ownershipTypesObj.size());
    int numIngested = 0;
    for (final JsonNode roleObj : ownershipTypesObj) {
      final Urn urn = Urn.createFromString(roleObj.get("urn").asText());
      final OwnershipTypeInfo info = RecordUtils.toRecordTemplate(OwnershipTypeInfo.class, roleObj.get("info")
          .toString());
      log.info(String.format("Ingesting default ownership type with urn %s", urn));
      ingestOwnershipType(urn, info, auditStamp, ownershipTypeInfoAspectSpec);
      numIngested++;
    }
    log.info("Ingested {} new ownership types", numIngested);
  }

  private void ingestOwnershipType(final Urn ownershipTypeUrn, final OwnershipTypeInfo info, final AuditStamp auditStamp,
      final AspectSpec ownershipTypeInfoAspectSpec) {

    // 3. Write key & aspect MCPs.
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(ownershipTypeUrn);
    GenericAspect aspect =
        GenericRecordUtils.serializeAspect(EntityKeyUtils.convertUrnToEntityKey(ownershipTypeUrn, keyAspectSpec));
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

    _entityService.ingestProposal(AspectsBatchImpl.builder()
            .mcps(List.of(keyAspectProposal, proposal), _entityService.getEntityRegistry()).build(), auditStamp,
            false);
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }
}
