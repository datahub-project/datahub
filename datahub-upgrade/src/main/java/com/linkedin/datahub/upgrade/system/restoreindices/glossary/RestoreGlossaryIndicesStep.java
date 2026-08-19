package com.linkedin.datahub.upgrade.system.restoreindices.glossary;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreGlossaryIndicesStep implements UpgradeStep {

  // STEP_ID and VERSION must match the values used when this step ran as a GMS boot step.
  // The idempotency check derives a URN from STEP_ID — changing either value causes all existing
  // deployments to re-run the migration because the prior completion record won't be found.
  private static final String STEP_ID = "restore-glossary-indices-ui";
  private static final String VERSION = "1";
  private static final Integer BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;
  private final EntitySearchService _entitySearchService;
  private final Urn _upgradeUrn;

  public RestoreGlossaryIndicesStep(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntitySearchService entitySearchService) {
    _entityService = entityService;
    _entitySearchService = entitySearchService;
    _upgradeUrn =
        EntityKeyUtils.convertEntityKeyToUrn(
            new DataHubUpgradeKey().setId(STEP_ID), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    try {
      EntityResponse response =
          _entityService.getEntityV2(
              context.opContext(),
              Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
              _upgradeUrn,
              Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)) {
        DataMap dataMap =
            response
                .getAspects()
                .get(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)
                .getValue()
                .data();
        DataHubUpgradeRequest request = new DataHubUpgradeRequest(dataMap);
        if (request.hasVersion() && request.getVersion().equals(VERSION)) {
          log.info("Step {} version {} already completed. Skipping.", STEP_ID, VERSION);
          return true;
        }
      }
    } catch (Exception e) {
      log.error("Error checking upgrade history for {}. Proceeding with upgrade.", STEP_ID, e);
    }
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        ingestUpgradeRequest(context.opContext());
        execute(context.opContext());
        ingestUpgradeResult(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to restore glossary indices", e);
        _entityService.deleteUrn(context.opContext(), _upgradeUrn);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void ingestUpgradeRequest(@Nonnull final OperationContext opContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(_upgradeUrn);
    proposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    proposal.setAspectName(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(
            new DataHubUpgradeRequest()
                .setTimestampMs(System.currentTimeMillis())
                .setVersion(VERSION)));
    proposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);
    _entityService.ingestProposal(opContext, proposal, auditStamp, false);
  }

  private void ingestUpgradeResult(@Nonnull final OperationContext opContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(_upgradeUrn);
    proposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    proposal.setAspectName(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(
            new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis())));
    proposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);
    _entityService.ingestProposal(opContext, proposal, auditStamp, false);
  }

  private void execute(@Nonnull final OperationContext systemOperationContext) throws Exception {
    final AspectSpec termAspectSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(Constants.GLOSSARY_TERM_ENTITY_NAME)
            .getAspectSpec(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    final AspectSpec nodeAspectSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(Constants.GLOSSARY_NODE_ENTITY_NAME)
            .getAspectSpec(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME);
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    final int totalTermsCount =
        getAndRestoreTermAspectIndices(systemOperationContext, 0, auditStamp, termAspectSpec);
    int termsCount = BATCH_SIZE;
    while (termsCount < totalTermsCount) {
      getAndRestoreTermAspectIndices(
          systemOperationContext, termsCount, auditStamp, termAspectSpec);
      termsCount += BATCH_SIZE;
    }

    final int totalNodesCount =
        getAndRestoreNodeAspectIndices(systemOperationContext, 0, auditStamp, nodeAspectSpec);
    int nodesCount = BATCH_SIZE;
    while (nodesCount < totalNodesCount) {
      getAndRestoreNodeAspectIndices(
          systemOperationContext, nodesCount, auditStamp, nodeAspectSpec);
      nodesCount += BATCH_SIZE;
    }
  }

  private int getAndRestoreTermAspectIndices(
      @Nonnull OperationContext systemOperationContext,
      int start,
      AuditStamp auditStamp,
      AspectSpec termAspectSpec)
      throws Exception {
    SearchResult termsResult =
        _entitySearchService.search(
            systemOperationContext.withSearchFlags(
                flags ->
                    flags.setFulltext(false).setSkipAggregates(true).setSkipHighlighting(true)),
            List.of(Constants.GLOSSARY_TERM_ENTITY_NAME),
            "",
            null,
            null,
            start,
            BATCH_SIZE);
    List<Urn> termUrns =
        termsResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toList());
    if (termUrns.isEmpty()) {
      return 0;
    }
    final Map<Urn, EntityResponse> termInfoResponses =
        _entityService.getEntitiesV2(
            systemOperationContext,
            Constants.GLOSSARY_TERM_ENTITY_NAME,
            new HashSet<>(termUrns),
            Collections.singleton(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME));

    List<Future<?>> futures = new LinkedList<>();
    for (Urn termUrn : termUrns) {
      EntityResponse termEntityResponse = termInfoResponses.get(termUrn);
      if (termEntityResponse == null) {
        log.warn("Term not in set of entity responses {}", termUrn);
        continue;
      }
      GlossaryTermInfo termInfo = mapTermInfo(termEntityResponse);
      if (termInfo == null) {
        log.warn("Received null termInfo for urn {}", termUrn);
        continue;
      }

      futures.add(
          _entityService
              .alwaysProduceMCLAsync(
                  systemOperationContext,
                  termUrn,
                  Constants.GLOSSARY_TERM_ENTITY_NAME,
                  Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
                  termAspectSpec,
                  null,
                  termInfo,
                  null,
                  null,
                  auditStamp,
                  ChangeType.RESTATE)
              .getFirst());
    }

    futures.stream()
        .filter(Objects::nonNull)
        .forEach(
            f -> {
              try {
                f.get();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            });

    return termsResult.getNumEntities();
  }

  private int getAndRestoreNodeAspectIndices(
      @Nonnull OperationContext systemOperationContext,
      int start,
      AuditStamp auditStamp,
      AspectSpec nodeAspectSpec)
      throws Exception {
    SearchResult nodesResult =
        _entitySearchService.search(
            systemOperationContext.withSearchFlags(
                flags ->
                    flags.setFulltext(false).setSkipAggregates(true).setSkipHighlighting(true)),
            List.of(Constants.GLOSSARY_NODE_ENTITY_NAME),
            "",
            null,
            null,
            start,
            BATCH_SIZE);
    List<Urn> nodeUrns =
        nodesResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toList());
    if (nodeUrns.isEmpty()) {
      return 0;
    }
    final Map<Urn, EntityResponse> nodeInfoResponses =
        _entityService.getEntitiesV2(
            systemOperationContext,
            Constants.GLOSSARY_NODE_ENTITY_NAME,
            new HashSet<>(nodeUrns),
            Collections.singleton(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME));

    List<Future<?>> futures = new LinkedList<>();
    for (Urn nodeUrn : nodeUrns) {
      EntityResponse nodeEntityResponse = nodeInfoResponses.get(nodeUrn);
      if (nodeEntityResponse == null) {
        log.warn("Node not in set of entity responses {}", nodeUrn);
        continue;
      }
      GlossaryNodeInfo nodeInfo = mapNodeInfo(nodeEntityResponse);
      if (nodeInfo == null) {
        log.warn("Received null nodeInfo for urn {}", nodeUrn);
        continue;
      }

      futures.add(
          _entityService
              .alwaysProduceMCLAsync(
                  systemOperationContext,
                  nodeUrn,
                  Constants.GLOSSARY_NODE_ENTITY_NAME,
                  Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
                  nodeAspectSpec,
                  null,
                  nodeInfo,
                  null,
                  null,
                  auditStamp,
                  ChangeType.RESTATE)
              .getFirst());
    }

    futures.stream()
        .filter(Objects::nonNull)
        .forEach(
            f -> {
              try {
                f.get();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            });

    return nodesResult.getNumEntities();
  }

  private GlossaryTermInfo mapTermInfo(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)) {
      return null;
    }
    return new GlossaryTermInfo(
        aspectMap.get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME).getValue().data());
  }

  private GlossaryNodeInfo mapNodeInfo(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME)) {
      return null;
    }
    return new GlossaryNodeInfo(
        aspectMap.get(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME).getValue().data());
  }
}
