package com.linkedin.metadata.boot.steps;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.io.IOException;
import java.net.URISyntaxException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class IngestGroupsStep implements BootstrapStep {
  private final EntityService _entityService;

  @Override
  public String name() {
    return "IngestGroupsStep";
  }

  @Override
  public void execute() throws IOException, URISyntaxException {

    final ObjectMapper mapper = new ObjectMapper();

    // 0. Execute preflight check to see whether we need to ingest groups
    log.info("Ingesting default groups...");

    // 1. Read from the file into JSON.
    final JsonNode groupsObj = mapper.readTree(new ClassPathResource("./boot/groups.json").getFile());

    if (!groupsObj.isArray()) {
      throw new RuntimeException(
          String.format("Found malformed groups file, expected an Array but found %s", groupsObj.getNodeType()));
    }

    // 2. For each JSON object, cast into a CorpGroupInfo object.
    for (final JsonNode groupObj : groupsObj) {
      final Urn urn = Urn.createFromString(groupObj.get("urn").asText());
      final CorpGroupInfo info = RecordUtils.toRecordTemplate(CorpGroupInfo.class, groupObj.get("info").toString());

      log.info(String.format("Ingesting default group with urn %s", urn));
      ingestGroup(urn, info);
    }
  }

  private void ingestGroup(final Urn urn, final CorpGroupInfo info) throws URISyntaxException {
    // 3. Write key & aspect
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(urn);
    GenericAspect aspect =
        GenericRecordUtils.serializeAspect(EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec.getPegasusSchema()));
    keyAspectProposal.setAspect(aspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(CORP_GROUP_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(urn);

    _entityService.ingestProposal(keyAspectProposal,
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(CORP_GROUP_ENTITY_NAME);
    proposal.setAspectName(CORP_GROUP_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(proposal,
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));
  }
}
