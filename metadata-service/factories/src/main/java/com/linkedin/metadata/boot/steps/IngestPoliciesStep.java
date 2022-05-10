package com.linkedin.metadata.boot.steps;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubPolicyInfo;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;


@Slf4j
@RequiredArgsConstructor
public class IngestPoliciesStep implements BootstrapStep {

  private static final String POLICY_ENTITY_NAME = "dataHubPolicy";
  private static final String POLICY_INFO_ASPECT_NAME = "dataHubPolicyInfo";

  private final EntityRegistry _entityRegistry;
  private final EntityService _entityService;
  private final EntitySearchService _entitySearchService;
  private final SearchDocumentTransformer _searchDocumentTransformer;

  @Override
  public String name() {
    return "IngestPoliciesStep";
  }

  @Override
  public void execute() throws IOException, URISyntaxException {

    final ObjectMapper mapper = new ObjectMapper();

    // 0. Execute preflight check to see whether we need to ingest policies
    log.info("Ingesting default access policies...");

    // 1. Read from the file into JSON.
    final JsonNode policiesObj = mapper.readTree(new ClassPathResource("./boot/policies.json").getFile());

    if (!policiesObj.isArray()) {
      throw new RuntimeException(
          String.format("Found malformed policies file, expected an Array but found %s", policiesObj.getNodeType()));
    }

    // 2. For each JSON object, cast into a DataHub Policy Info object.
    for (final JsonNode policyObj : policiesObj) {
      final Urn urn = Urn.createFromString(policyObj.get("urn").asText());

      // If the info is not there, it means that the policy was there before, but must now be removed
      if (!policyObj.has("info")) {
        _entityService.deleteUrn(urn);
        continue;
      }

      final DataHubPolicyInfo info =
          RecordUtils.toRecordTemplate(DataHubPolicyInfo.class, policyObj.get("info").toString());

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
    // If search index for policies is empty, update the policy index with the ingested policies from previous step.
    // Directly update the ES index, does not produce MCLs
    if (_entitySearchService.docCount(Constants.POLICY_ENTITY_NAME) == 0) {
      updatePolicyIndex();
    }
    log.info("Successfully ingested default access policies.");
  }

  /**
   * Update policy index and push in the relevant search documents into the search index
   */
  private void updatePolicyIndex() throws URISyntaxException {
    log.info("Pushing documents to the policy index");
    AspectSpec policyInfoAspectSpec = _entityRegistry.getEntitySpec(Constants.POLICY_ENTITY_NAME)
        .getAspectSpec(Constants.DATAHUB_POLICY_INFO_ASPECT_NAME);
    int start = 0;
    int count = 30;
    int total = 100;
    while (start < total) {
      ListUrnsResult listUrnsResult = _entityService.listUrns(Constants.POLICY_ENTITY_NAME, start, count);
      total = listUrnsResult.getTotal();
      start = start + count;

      final Map<Urn, EntityResponse> policyEntities =
          _entityService.getEntitiesV2(POLICY_ENTITY_NAME, new HashSet<>(listUrnsResult.getEntities()),
              Collections.singleton(Constants.DATAHUB_POLICY_INFO_ASPECT_NAME));
      policyEntities.values().forEach(entityResponse -> insertPolicyDocument(entityResponse, policyInfoAspectSpec));
    }
    log.info("Successfully updated the policy index");
  }

  private void insertPolicyDocument(EntityResponse entityResponse, AspectSpec aspectSpec) {
    EnvelopedAspect aspect = entityResponse.getAspects().get(Constants.DATAHUB_POLICY_INFO_ASPECT_NAME);
    if (aspect == null) {
      throw new RuntimeException(String.format("Missing policy info aspect for urn %s", entityResponse.getUrn()));
    }

    Optional<String> searchDocument;
    try {
      searchDocument = _searchDocumentTransformer.transformAspect(entityResponse.getUrn(),
          new DataHubPolicyInfo(aspect.getValue().data()), aspectSpec, false);
    } catch (Exception e) {
      log.error("Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
      return;
    }

    if (!searchDocument.isPresent()) {
      return;
    }

    Optional<String> docId = SearchUtils.getDocId(entityResponse.getUrn());

    if (!docId.isPresent()) {
      return;
    }

    _entitySearchService.upsertDocument(Constants.POLICY_ENTITY_NAME, searchDocument.get(), docId.get());
  }

  private void ingestPolicy(final Urn urn, final DataHubPolicyInfo info) throws URISyntaxException {
    // 3. Write key & aspect
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(urn);
    GenericAspect aspect =
        GenericRecordUtils.serializeAspect(EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec.getPegasusSchema()));
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

  private boolean hasPolicy(Urn policyUrn) {
    // Check if policy exists
    RecordTemplate aspect = _entityService.getAspect(policyUrn, POLICY_INFO_ASPECT_NAME, 0);
    return aspect != null;
  }
}
