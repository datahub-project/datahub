package com.linkedin.metadata.boot.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubPolicyInfo;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import org.springframework.core.io.ClassPathResource;


public class IngestPoliciesStep implements BootstrapStep {

  private static final String POLICY_ENTITY_NAME = "dataHubPolicy";
  private static final String POLICY_INFO_ASPECT_NAME = "dataHubPolicyInfo";

  private final EntityService _entityService;

  public IngestPoliciesStep(final EntityService entityService) {
    _entityService = entityService;
  }

  @Override
  public String name() {
    return "IngestPoliciesStep";
  }

  @Override
  public void execute() throws IOException, URISyntaxException {

    final ObjectMapper mapper = new ObjectMapper();

    // 1. Read from the file into JSON.
    final JsonNode policiesObj = mapper.readTree(new ClassPathResource("./boot/policies.json").getFile());

    if (!policiesObj.isArray()) {
      throw new RuntimeException(String.format("Found malformed policies file, expected an Array but found %s", policiesObj.getNodeType()));
    }

    // 2. For each JSON object, cast into a DataHub Policy Info object.
    for (Iterator<JsonNode> it = policiesObj.iterator(); it.hasNext(); ) {
      final JsonNode policyObj = it.next();
      final DataHubPolicyInfo info = RecordUtils.toRecordTemplate(DataHubPolicyInfo.class, policyObj.get("info").toString());
      final Urn urn = Urn.createFromString(policyObj.get("urn").asText());

      // 3. Write key & aspect
      final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
      final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(urn);
      GenericAspect aspect = GenericAspectUtils.serializeAspect(
          EntityKeyUtils.convertUrnToEntityKey(
              urn, keyAspectSpec.getPegasusSchema()));
      keyAspectProposal.setAspect(aspect);
      keyAspectProposal.setAspectName(keyAspectSpec.getName());
      keyAspectProposal.setEntityType(POLICY_ENTITY_NAME);
      keyAspectProposal.setChangeType(ChangeType.UPSERT);
      keyAspectProposal.setEntityUrn(urn);

      _entityService.ingestProposal(keyAspectProposal, new AuditStamp()
          .setActor(Urn.createFromString("urn:li:corpuser:system"))
          .setTime(System.currentTimeMillis()));

      final MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(urn);
      proposal.setEntityType(POLICY_ENTITY_NAME);
      proposal.setAspectName(POLICY_INFO_ASPECT_NAME);
      proposal.setAspect(GenericAspectUtils.serializeAspect(info));
      proposal.setChangeType(ChangeType.UPSERT);

      _entityService.ingestProposal(proposal, new AuditStamp()
          .setActor(Urn.createFromString("urn:li:corpuser:system"))
          .setTime(System.currentTimeMillis()));
    }
  }
}
