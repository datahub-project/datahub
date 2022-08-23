package com.linkedin.metadata.boot.steps;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
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
import com.linkedin.policy.DataHubRoleInfo;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class IngestRolesStep implements BootstrapStep {

  private final EntityRegistry _entityRegistry;
  private final EntityService _entityService;
  private final EntitySearchService _entitySearchService;
  private final SearchDocumentTransformer _searchDocumentTransformer;

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute() throws Exception {
    final ObjectMapper mapper = new ObjectMapper();

    // 0. Execute preflight check to see whether we need to ingest Roles
    log.info("Ingesting default Roles...");

    // 1. Read from the file into JSON.
    final JsonNode rolesObj = mapper.readTree(new ClassPathResource("./boot/roles.json").getFile());

    if (!rolesObj.isArray()) {
      throw new RuntimeException(
          String.format("Found malformed roles file, expected an Array but found %s", rolesObj.getNodeType()));
    }

    for (final JsonNode roleObj : rolesObj) {
      final Urn urn = Urn.createFromString(roleObj.get("urn").asText());

      // If the info is not there, it means that the role was there before, but must now be removed
      if (!roleObj.has("info")) {
        _entityService.deleteUrn(urn);
        continue;
      }

      final DataHubRoleInfo info = RecordUtils.toRecordTemplate(DataHubRoleInfo.class, roleObj.get("info").toString());
      ingestRole(urn, info);
    }
    // If search index for roles is empty, update the role index with the ingested roles from previous step.
    // Directly update the ES index, does not produce MCLs
    if (_entitySearchService.docCount(DATAHUB_ROLE_ENTITY_NAME) == 0) {
      updateRoleIndex();
    }
    log.info("Successfully ingested default Roles.");
  }

  private void ingestRole(final Urn urn, final DataHubRoleInfo info) throws URISyntaxException {
    // 3. Write key & aspect
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(urn);
    GenericAspect aspect =
        GenericRecordUtils.serializeAspect(EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec.getPegasusSchema()));
    keyAspectProposal.setAspect(aspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(DATAHUB_ROLE_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(urn);

    _entityService.ingestProposal(keyAspectProposal,
        new AuditStamp().setActor(Urn.createFromString(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(DATAHUB_ROLE_ENTITY_NAME);
    proposal.setAspectName(DATAHUB_ROLE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(proposal,
        new AuditStamp().setActor(Urn.createFromString(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));
  }

  /**
   * Update Role index and push in the relevant search documents into the search index
   */
  private void updateRoleIndex() throws URISyntaxException {
    log.info("Pushing documents to the Role index");
    AspectSpec roleInfoAspectSpec =
        _entityRegistry.getEntitySpec(DATAHUB_ROLE_ENTITY_NAME).getAspectSpec(DATAHUB_ROLE_INFO_ASPECT_NAME);
    int start = 0;
    int count = 30;
    int total = 100;
    while (start < total) {
      ListUrnsResult listUrnsResult = _entityService.listUrns(DATAHUB_ROLE_ENTITY_NAME, start, count);
      total = listUrnsResult.getTotal();
      start = start + count;

      final Map<Urn, EntityResponse> roleEntities =
          _entityService.getEntitiesV2(DATAHUB_ROLE_ENTITY_NAME, new HashSet<>(listUrnsResult.getEntities()),
              Collections.singleton(DATAHUB_ROLE_INFO_ASPECT_NAME));
      roleEntities.values().forEach(entityResponse -> insertRoleDocument(entityResponse, roleInfoAspectSpec));
    }
    log.info("Successfully updated the Role index");
  }

  private void insertRoleDocument(EntityResponse entityResponse, AspectSpec aspectSpec) {
    EnvelopedAspect aspect = entityResponse.getAspects().get(DATAHUB_ROLE_INFO_ASPECT_NAME);
    if (aspect == null) {
      log.info("Missing DataHubRoleInfo aspect for urn {}", entityResponse.getUrn());
      return;
    }

    Optional<String> searchDocument;
    try {
      searchDocument = _searchDocumentTransformer.transformAspect(entityResponse.getUrn(),
          new DataHubRoleInfo(aspect.getValue().data()), aspectSpec, false);
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

    _entitySearchService.upsertDocument(DATAHUB_ROLE_ENTITY_NAME, searchDocument.get(), docId.get());
  }
}
