package com.linkedin.datahub.upgrade.system.restoreindices.glossary;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
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
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
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

  private static final String STEP_ID = "restore-glossary-indices-ui";
  private static final Integer BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;
  private final EntitySearchService _entitySearchService;

  public RestoreGlossaryIndicesStep(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntitySearchService entitySearchService) {
    _entityService = entityService;
    _entitySearchService = entitySearchService;
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
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        execute(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to restore glossary indices", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
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
