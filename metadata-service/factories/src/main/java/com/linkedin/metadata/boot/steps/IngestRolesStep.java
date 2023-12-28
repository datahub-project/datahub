package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
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
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubRoleInfo;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

@Slf4j
@RequiredArgsConstructor
public class IngestRolesStep implements BootstrapStep {
  private static final int SLEEP_SECONDS = 60;
  private final EntityService _entityService;
  private final EntityRegistry _entityRegistry;

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  @Override
  public void execute() throws Exception {
    final ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());

    // Sleep to ensure deployment process finishes.
    Thread.sleep(SLEEP_SECONDS * 1000);

    // 0. Execute preflight check to see whether we need to ingest Roles
    log.info("Ingesting default Roles...");

    // 1. Read from the file into JSON.
    final JsonNode rolesObj = mapper.readTree(new ClassPathResource("./boot/roles.json").getFile());

    if (!rolesObj.isArray()) {
      throw new RuntimeException(
          String.format(
              "Found malformed roles file, expected an Array but found %s",
              rolesObj.getNodeType()));
    }

    final AspectSpec roleInfoAspectSpec =
        _entityRegistry
            .getEntitySpec(DATAHUB_ROLE_ENTITY_NAME)
            .getAspectSpec(DATAHUB_ROLE_INFO_ASPECT_NAME);
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    for (final JsonNode roleObj : rolesObj) {
      final Urn urn = Urn.createFromString(roleObj.get("urn").asText());

      // If the info is not there, it means that the role was there before, but must now be removed
      if (!roleObj.has("info")) {
        _entityService.deleteUrn(urn);
        continue;
      }

      final DataHubRoleInfo info =
          RecordUtils.toRecordTemplate(DataHubRoleInfo.class, roleObj.get("info").toString());
      ingestRole(urn, info, auditStamp, roleInfoAspectSpec);
    }

    log.info("Successfully ingested default Roles.");
  }

  private void ingestRole(
      final Urn roleUrn,
      final DataHubRoleInfo dataHubRoleInfo,
      final AuditStamp auditStamp,
      final AspectSpec roleInfoAspectSpec)
      throws URISyntaxException {
    // 3. Write key & aspect
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(roleUrn);
    GenericAspect aspect =
        GenericRecordUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(roleUrn, keyAspectSpec));
    keyAspectProposal.setAspect(aspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(DATAHUB_ROLE_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(roleUrn);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(roleUrn);
    proposal.setEntityType(DATAHUB_ROLE_ENTITY_NAME);
    proposal.setAspectName(DATAHUB_ROLE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dataHubRoleInfo));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(
        AspectsBatchImpl.builder()
            .mcps(List.of(keyAspectProposal, proposal), _entityRegistry)
            .build(),
        new AuditStamp()
            .setActor(Urn.createFromString(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()),
        false);

    _entityService.alwaysProduceMCLAsync(
        roleUrn,
        DATAHUB_ROLE_ENTITY_NAME,
        DATAHUB_ROLE_INFO_ASPECT_NAME,
        roleInfoAspectSpec,
        null,
        dataHubRoleInfo,
        null,
        null,
        auditStamp,
        ChangeType.RESTATE);
  }
}
