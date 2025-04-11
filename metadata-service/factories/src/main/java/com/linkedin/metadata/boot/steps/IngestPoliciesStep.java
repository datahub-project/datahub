package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
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
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;

@Slf4j
@RequiredArgsConstructor
public class IngestPoliciesStep implements BootstrapStep {

  private static final String POLICY_ENTITY_NAME = "dataHubPolicy";
  private static final String POLICY_INFO_ASPECT_NAME = "dataHubPolicyInfo";

  private final EntityService<?> _entityService;
  private final EntitySearchService _entitySearchService;
  private final SearchDocumentTransformer _searchDocumentTransformer;

  private final Resource _policiesResource;

  @Override
  public String name() {
    return "IngestPoliciesStep";
  }

  @Override
  public void execute(@Nonnull OperationContext systemOperationContext)
      throws IOException, URISyntaxException {

    final ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());

    // 0. Execute preflight check to see whether we need to ingest policies
    log.info("Ingesting default access policies from: {}...", _policiesResource);

    // 1. Read from the file into JSON.
    final JsonNode policiesObj = mapper.readTree(_policiesResource.getInputStream());

    if (!policiesObj.isArray()) {
      throw new RuntimeException(
          String.format(
              "Found malformed policies file, expected an Array but found %s",
              policiesObj.getNodeType()));
    }

    // 2. For each JSON object, cast into a DataHub Policy Info object.
    for (final JsonNode policyObj : policiesObj) {
      final Urn urn = Urn.createFromString(policyObj.get("urn").asText());

      // If the info is not there, it means that the policy was there before, but must now be
      // removed
      if (!policyObj.has("info")) {
        _entityService.deleteUrn(systemOperationContext, urn);
        continue;
      }

      final DataHubPolicyInfo info =
          RecordUtils.toRecordTemplate(DataHubPolicyInfo.class, policyObj.get("info").toString());

      if (!info.isEditable()) {
        // If the Policy is not editable, always re-ingest.
        log.info(String.format("Ingesting default policy with urn %s", urn));
        ingestPolicy(systemOperationContext, urn, info);
      } else {
        // If the Policy is editable (ie. an example policy), only ingest on a clean boot up.
        if (!hasPolicy(systemOperationContext, urn)) {
          log.info(String.format("Ingesting default policy with urn %s", urn));
          ingestPolicy(systemOperationContext, urn, info);
        } else {
          log.info(String.format("Skipping ingestion of editable policy with urn %s", urn));
        }
      }
    }
    // If search index for policies is empty, update the policy index with the ingested policies
    // from previous step.
    // Directly update the ES index, does not produce MCLs
    if (_entitySearchService.docCount(systemOperationContext, Constants.POLICY_ENTITY_NAME) == 0) {
      updatePolicyIndex(systemOperationContext);
    }
    log.info("Successfully ingested default access policies.");
  }

  /** Update policy index and push in the relevant search documents into the search index */
  private void updatePolicyIndex(@Nonnull OperationContext systemOperationContext)
      throws URISyntaxException {
    log.info("Pushing documents to the policy index");
    AspectSpec policyInfoAspectSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(Constants.POLICY_ENTITY_NAME)
            .getAspectSpec(Constants.DATAHUB_POLICY_INFO_ASPECT_NAME);
    int start = 0;
    int count = 30;
    int total = 100;
    while (start < total) {
      ListUrnsResult listUrnsResult =
          _entityService.listUrns(
              systemOperationContext, Constants.POLICY_ENTITY_NAME, start, count);
      total = listUrnsResult.getTotal();
      start = start + count;

      final Map<Urn, EntityResponse> policyEntities =
          _entityService.getEntitiesV2(
              systemOperationContext,
              POLICY_ENTITY_NAME,
              new HashSet<>(listUrnsResult.getEntities()),
              Collections.singleton(Constants.DATAHUB_POLICY_INFO_ASPECT_NAME));
      policyEntities
          .values()
          .forEach(
              entityResponse ->
                  insertPolicyDocument(
                      systemOperationContext, entityResponse, policyInfoAspectSpec));
    }
    log.info("Successfully updated the policy index");
  }

  private void insertPolicyDocument(
      @Nonnull OperationContext systemOperationContext,
      EntityResponse entityResponse,
      AspectSpec aspectSpec) {
    EnvelopedAspect aspect =
        entityResponse.getAspects().get(Constants.DATAHUB_POLICY_INFO_ASPECT_NAME);
    if (aspect == null) {
      log.info("Missing policy info aspect for urn {}", entityResponse.getUrn());
      return;
    }

    Optional<String> searchDocument;
    try {
      searchDocument =
          _searchDocumentTransformer
              .transformAspect(
                  systemOperationContext,
                  entityResponse.getUrn(),
                  new DataHubPolicyInfo(aspect.getValue().data()),
                  aspectSpec,
                  false,
                  AuditStampUtils.createDefaultAuditStamp())
              .map(Objects::toString);
    } catch (Exception e) {
      log.error(
          "Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
      return;
    }

    if (!searchDocument.isPresent()) {
      return;
    }

    final String docId =
        _entitySearchService.getIndexConvention().getEntityDocumentId(entityResponse.getUrn());

    _entitySearchService.upsertDocument(
        systemOperationContext, Constants.POLICY_ENTITY_NAME, searchDocument.get(), docId);
  }

  private void ingestPolicy(
      @Nonnull OperationContext systemOperationContext, final Urn urn, final DataHubPolicyInfo info)
      throws URISyntaxException {
    // 3. Write key & aspect
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec =
        systemOperationContext.getEntityRegistryContext().getKeyAspectSpec(urn);
    GenericAspect aspect =
        GenericRecordUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec));
    keyAspectProposal.setAspect(aspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(POLICY_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(urn);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(POLICY_ENTITY_NAME);
    proposal.setAspectName(POLICY_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(
        systemOperationContext,
        AspectsBatchImpl.builder()
            .mcps(
                List.of(keyAspectProposal, proposal),
                new AuditStamp()
                    .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                    .setTime(System.currentTimeMillis()),
                systemOperationContext.getRetrieverContext())
            .build(),
        false);
  }

  private boolean hasPolicy(@Nonnull OperationContext systemOperationContext, Urn policyUrn) {
    // Check if policy exists
    RecordTemplate aspect =
        _entityService.getAspect(systemOperationContext, policyUrn, POLICY_INFO_ASPECT_NAME, 0);
    return aspect != null;
  }
}
