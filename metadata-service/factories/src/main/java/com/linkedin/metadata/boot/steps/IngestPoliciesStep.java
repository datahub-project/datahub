package com.linkedin.metadata.boot.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.events.metadata.ChangeType;
import com.datahub.util.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubPolicyInfo;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;


@Slf4j
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

    // 0. Execute preflight check to see whether we need to ingest policies
    log.info("Ingesting default access policies...");

    // Whether we are at clean boot or not.
    final boolean hasDefaultPolicies = hasDefaultPolicies();

    // 1. Read from the file into JSON.
    final JsonNode policiesObj = mapper.readTree(new ClassPathResource("./boot/policies.json").getFile());

    if (!policiesObj.isArray()) {
      throw new RuntimeException(String.format("Found malformed policies file, expected an Array but found %s", policiesObj.getNodeType()));
    }

    // 2. For each JSON object, cast into a DataHub Policy Info object.
    for (Iterator<JsonNode> it = policiesObj.iterator(); it.hasNext(); ) {
      final JsonNode policyObj = it.next();
      final Urn urn = Urn.createFromString(policyObj.get("urn").asText());

      // If the info is not there, it means that the policy was there before, but must now be removed
      if (!policyObj.has("info")) {
        _entityService.deleteUrn(urn);
        continue;
      }

      final DataHubPolicyInfo info = RecordUtils.toRecordTemplate(DataHubPolicyInfo.class, policyObj.get("info").toString());

      if (!info.isEditable()) {
        // If the Policy is not editable, always re-ingest.
        log.info(String.format("Ingesting default policy with urn %s", urn));
        ingestPolicy(urn, info);
      } else {
        // If the Policy is editable (ie. an example policy), only ingest on a clean boot up.
        if (!hasPolicy(urn)) {
          log.info(String.format("Ingesting default policy with urn %s", urn));
          ingestPolicy(urn, info);
        } else {
          log.info(String.format("Skipping ingestion of editable policy with urn %s", urn));
        }
      }
    }
    log.info("Successfully ingested default access policies.");
  }

  private void ingestPolicy(final Urn urn, final DataHubPolicyInfo info) throws URISyntaxException {
    // 3. Write key & aspect
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(urn);
    GenericAspect aspect = GenericRecordUtils.serializeAspect(EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec.getPegasusSchema()));
    keyAspectProposal.setAspect(aspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(POLICY_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(urn);

    _entityService.ingestProposal(keyAspectProposal,
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(POLICY_ENTITY_NAME);
    proposal.setAspectName(POLICY_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(proposal,
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));
  }

  private boolean hasDefaultPolicies() throws URISyntaxException {
    // If there are already default policies, denoted by presence of policy 0, don't ingest bootstrap policies.
    // This will retain any changes made to policies after initial bootstrap.
    final Urn defaultPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:0");
    RecordTemplate aspect = _entityService.getAspect(defaultPolicyUrn, POLICY_INFO_ASPECT_NAME, 0);
    return aspect != null;
  }

  private boolean hasPolicy(Urn policyUrn) {
    // Check if policy exists
    RecordTemplate aspect = _entityService.getAspect(policyUrn, POLICY_INFO_ASPECT_NAME, 0);
    return aspect != null;
  }
}
