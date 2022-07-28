package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class RestoreGlossaryIndices extends UpgradeStep {
  private static final String VERSION = "1";
  private static final String UPGRADE_ID = "restore-glossary-indices-ui";
  private static final Integer BATCH_SIZE = 1000;

  private final EntitySearchService _entitySearchService;
  private final EntityRegistry _entityRegistry;

  public RestoreGlossaryIndices(EntityService entityService, EntitySearchService entitySearchService,
      EntityRegistry entityRegistry) {
    super(entityService, VERSION, UPGRADE_ID);
    _entitySearchService = entitySearchService;
    _entityRegistry = entityRegistry;
  }

  @Override
  public void upgrade() throws Exception {
    final AspectSpec termAspectSpec = _entityRegistry.getEntitySpec(Constants.GLOSSARY_TERM_ENTITY_NAME)
        .getAspectSpec(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    final AspectSpec nodeAspectSpec = _entityRegistry.getEntitySpec(Constants.GLOSSARY_NODE_ENTITY_NAME)
        .getAspectSpec(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME);
    final AuditStamp auditStamp =
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

    final int totalTermsCount = getAndRestoreTermAspectIndices(0, auditStamp, termAspectSpec);
    int termsCount = BATCH_SIZE;
    while (termsCount < totalTermsCount) {
      getAndRestoreTermAspectIndices(termsCount, auditStamp, termAspectSpec);
      termsCount += BATCH_SIZE;
    }

    final int totalNodesCount = getAndRestoreNodeAspectIndices(0, auditStamp, nodeAspectSpec);
    int nodesCount = BATCH_SIZE;
    while (nodesCount < totalNodesCount) {
      getAndRestoreNodeAspectIndices(nodesCount, auditStamp, nodeAspectSpec);
      nodesCount += BATCH_SIZE;
    }
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  private int getAndRestoreTermAspectIndices(int start, AuditStamp auditStamp, AspectSpec termAspectSpec)
      throws Exception {
    SearchResult termsResult =
        _entitySearchService.search(Constants.GLOSSARY_TERM_ENTITY_NAME, "", null, null, start, BATCH_SIZE);
    List<Urn> termUrns = termsResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());
    if (termUrns.size() == 0) {
      return 0;
    }
    final Map<Urn, EntityResponse> termInfoResponses =
        _entityService.getEntitiesV2(Constants.GLOSSARY_TERM_ENTITY_NAME, new HashSet<>(termUrns),
        Collections.singleton(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
    );

    //  Loop over Terms and produce changelog
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

      _entityService.produceMetadataChangeLog(
          termUrn,
          Constants.GLOSSARY_TERM_ENTITY_NAME,
          Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
          termAspectSpec,
          null,
          termInfo,
          null,
          null,
          auditStamp,
          ChangeType.RESTATE);
    }

    return termsResult.getNumEntities();
  }

  private int getAndRestoreNodeAspectIndices(int start, AuditStamp auditStamp, AspectSpec nodeAspectSpec) throws Exception {
    SearchResult nodesResult = _entitySearchService.search(Constants.GLOSSARY_NODE_ENTITY_NAME, "", null, null, start, BATCH_SIZE);
    List<Urn> nodeUrns = nodesResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());
    if (nodeUrns.size() == 0) {
      return 0;
    }
    final Map<Urn, EntityResponse> nodeInfoResponses = _entityService.getEntitiesV2(
        Constants.GLOSSARY_NODE_ENTITY_NAME,
        new HashSet<>(nodeUrns),
        Collections.singleton(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME)
    );

    //  Loop over Nodes and produce changelog
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

      _entityService.produceMetadataChangeLog(
          nodeUrn,
          Constants.GLOSSARY_NODE_ENTITY_NAME,
          Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
          nodeAspectSpec,
          null,
          nodeInfo,
          null,
          null,
          auditStamp,
          ChangeType.RESTATE);
    }

    return nodesResult.getNumEntities();
  }

  private GlossaryTermInfo mapTermInfo(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)) {
      return null;
    }

    return new GlossaryTermInfo(aspectMap.get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME).getValue().data());
  }

  private GlossaryNodeInfo mapNodeInfo(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME)) {
      return null;
    }

    return new GlossaryNodeInfo(aspectMap.get(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME).getValue().data());
  }
}