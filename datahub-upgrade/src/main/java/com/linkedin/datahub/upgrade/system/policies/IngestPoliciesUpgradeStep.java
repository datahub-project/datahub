package com.linkedin.datahub.upgrade.system.policies;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
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
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;

/**
 * System-update blocking step that ingests default access policies on every deploy. Non-editable
 * system policies are always re-ingested; editable example policies are only ingested on clean
 * installs (when absent).
 */
@Slf4j
public class IngestPoliciesUpgradeStep implements UpgradeStep {

  private static final String STEP_ID = "ingest-policies";

  private final EntityService<?> _entityService;
  private final EntitySearchService _entitySearchService;
  private final SearchDocumentTransformer _searchDocumentTransformer;
  private final Resource _policiesResource;
  private final boolean _enabled;

  public IngestPoliciesUpgradeStep(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntitySearchService entitySearchService,
      @Nonnull final SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull final Resource policiesResource,
      final boolean enabled) {
    _entityService = entityService;
    _entitySearchService = entitySearchService;
    _searchDocumentTransformer = searchDocumentTransformer;
    _policiesResource = policiesResource;
    _enabled = enabled;
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean isOptional() {
    return false;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    if (!_enabled) {
      log.info("Ingest policies step is disabled; skipping.");
      return true;
    }
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        execute(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to ingest default access policies", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void execute(@Nonnull final OperationContext systemOperationContext) throws Exception {
    final ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());

    log.info("Ingesting default access policies from: {}...", _policiesResource);

    final JsonNode policiesObj = mapper.readTree(_policiesResource.getInputStream());

    if (!policiesObj.isArray()) {
      throw new RuntimeException(
          String.format(
              "Found malformed policies file, expected an Array but found %s",
              policiesObj.getNodeType()));
    }

    for (final JsonNode policyObj : policiesObj) {
      final Urn urn = Urn.createFromString(policyObj.get("urn").asText());

      if (!policyObj.has("info")) {
        _entityService.deleteUrn(systemOperationContext, urn);
        continue;
      }

      final DataHubPolicyInfo info =
          RecordUtils.toRecordTemplate(DataHubPolicyInfo.class, policyObj.get("info").toString());

      if (!info.isEditable()) {
        log.info("Ingesting default policy with urn {}", urn);
        ingestPolicy(systemOperationContext, urn, info);
      } else {
        if (!hasPolicy(systemOperationContext, urn)) {
          log.info("Ingesting default policy with urn {}", urn);
          ingestPolicy(systemOperationContext, urn, info);
        } else {
          log.info("Skipping ingestion of editable policy with urn {}", urn);
        }
      }
    }

    // If search index for policies is empty, update the policy index with the ingested policies.
    // Directly update the ES index, does not produce MCLs.
    // Postgres-backed EntitySearchService doesn't support direct writes
    // (UnsupportedOperationException) — skip gracefully so the step doesn't crash on
    // Postgres-only profiles.
    try {
      if (_entitySearchService.docCount(systemOperationContext, Constants.POLICY_ENTITY_NAME)
          == 0) {
        updatePolicyIndex(systemOperationContext);
      }
    } catch (UnsupportedOperationException e) {
      log.info(
          "Skipping policy search-index update; current EntitySearchService does not support direct writes ({}).",
          e.getMessage());
    }
    log.info("Successfully ingested default access policies.");
  }

  private void updatePolicyIndex(@Nonnull final OperationContext systemOperationContext)
      throws Exception {
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
      @Nonnull final OperationContext systemOperationContext,
      final EntityResponse entityResponse,
      final AspectSpec aspectSpec) {
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

    if (searchDocument.isEmpty()) {
      return;
    }

    final String docId =
        systemOperationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityDocumentId(entityResponse.getUrn());

    _entitySearchService.upsertDocument(
        systemOperationContext, Constants.POLICY_ENTITY_NAME, searchDocument.get(), docId);
  }

  private void ingestPolicy(
      @Nonnull final OperationContext systemOperationContext,
      final Urn urn,
      final DataHubPolicyInfo info)
      throws Exception {
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
    proposal.setAspectName(Constants.DATAHUB_POLICY_INFO_ASPECT_NAME);
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
            .build(systemOperationContext),
        false);
  }

  private boolean hasPolicy(
      @Nonnull final OperationContext systemOperationContext, final Urn policyUrn) {
    RecordTemplate aspect =
        _entityService.getAspect(
            systemOperationContext, policyUrn, Constants.DATAHUB_POLICY_INFO_ASPECT_NAME, 0);
    return aspect != null;
  }
}
