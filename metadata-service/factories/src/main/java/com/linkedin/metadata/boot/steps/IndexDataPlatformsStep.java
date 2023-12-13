package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.search.EntitySearchService;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexDataPlatformsStep extends UpgradeStep {
  private static final String VERSION = "1";
  private static final String UPGRADE_ID = "index-data-platforms";
  private static final Integer BATCH_SIZE = 1000;

  private final EntitySearchService _entitySearchService;
  private final EntityRegistry _entityRegistry;

  public IndexDataPlatformsStep(
      EntityService entityService,
      EntitySearchService entitySearchService,
      EntityRegistry entityRegistry) {
    super(entityService, VERSION, UPGRADE_ID);
    _entitySearchService = entitySearchService;
    _entityRegistry = entityRegistry;
  }

  @Override
  public void upgrade() throws Exception {
    final AspectSpec dataPlatformSpec =
        _entityRegistry
            .getEntitySpec(Constants.DATA_PLATFORM_ENTITY_NAME)
            .getAspectSpec(Constants.DATA_PLATFORM_INFO_ASPECT_NAME);

    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    getAndReIndexDataPlatforms(auditStamp, dataPlatformSpec);

    log.info("Successfully indexed data platform aspects");
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  private int getAndReIndexDataPlatforms(
      AuditStamp auditStamp, AspectSpec dataPlatformInfoAspectSpec) throws Exception {
    ListUrnsResult listResult =
        _entityService.listUrns(Constants.DATA_PLATFORM_ENTITY_NAME, 0, BATCH_SIZE);

    List<Urn> dataPlatformUrns = listResult.getEntities();

    if (dataPlatformUrns.size() == 0) {
      return 0;
    }

    final Map<Urn, EntityResponse> dataPlatformInfoResponses =
        _entityService.getEntitiesV2(
            Constants.DATA_PLATFORM_ENTITY_NAME,
            new HashSet<>(dataPlatformUrns),
            Collections.singleton(Constants.DATA_PLATFORM_INFO_ASPECT_NAME));

    //  Loop over Data platforms and produce changelog
    List<Future<?>> futures = new LinkedList<>();
    for (Urn dpUrn : dataPlatformUrns) {
      EntityResponse dataPlatformEntityResponse = dataPlatformInfoResponses.get(dpUrn);
      if (dataPlatformEntityResponse == null) {
        log.warn("Data Platform not in set of entity responses {}", dpUrn);
        continue;
      }

      DataPlatformInfo dpInfo = mapDpInfo(dataPlatformEntityResponse);
      if (dpInfo == null) {
        log.warn("Received null dataPlatformInfo aspect for urn {}", dpUrn);
        continue;
      }

      futures.add(
          _entityService
              .alwaysProduceMCLAsync(
                  dpUrn,
                  Constants.DATA_PLATFORM_ENTITY_NAME,
                  Constants.DATA_PLATFORM_INFO_ASPECT_NAME,
                  dataPlatformInfoAspectSpec,
                  null,
                  dpInfo,
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

    return listResult.getTotal();
  }

  private DataPlatformInfo mapDpInfo(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(Constants.DATA_PLATFORM_INFO_ASPECT_NAME)) {
      return null;
    }

    return new DataPlatformInfo(
        aspectMap.get(Constants.DATA_PLATFORM_INFO_ASPECT_NAME).getValue().data());
  }
}
