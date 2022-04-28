package com.linkedin.metadata.resources.entity;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.resources.entity.ResourceUtils.*;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;


/**
 * Single unified resource for fetching, updating, searching, & browsing versioned DataHub entities
 */
@Slf4j
@RestLiCollection(name = "entitiesVersionedV2", namespace = "com.linkedin.entity",
    keyTyperefClass = com.linkedin.common.versioned.VersionedUrn.class)
public class EntityVersionedV2Resource extends CollectionResourceTaskTemplate<com.linkedin.common.urn.VersionedUrn, EntityResponse> {

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @RestMethod.BatchGet
  @Nonnull
  @WithSpan
  public Task<Map<Urn, EntityResponse>> batchGetVersioned(
      @Nonnull Set<com.linkedin.common.urn.VersionedUrn> versionedUrnStrs,
      @QueryParam(PARAM_ENTITY_TYPE) @Nonnull String entityType,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    log.debug("BATCH GET VERSIONED V2 {}", versionedUrnStrs);
    if (versionedUrnStrs.size() <= 0) {
      return Task.value(Collections.emptyMap());
    }
    return RestliUtil.toTask(() -> {
      final Set<String> projectedAspects =
          aspectNames == null ? getAllAspectNames(_entityService, entityType) : new HashSet<>(Arrays.asList(aspectNames));
      try {
        return _entityService.getEntitiesVersionedV2(versionedUrnStrs.stream()
            .map(versionedUrnTyperef -> {
              VersionedUrn versionedUrn = new VersionedUrn().setUrn(UrnUtils.getUrn(versionedUrnTyperef.getUrn()));
              if (versionedUrnTyperef.getVersionStamp() != null) {
                versionedUrn.setVersionStamp(versionedUrnTyperef.getVersionStamp());
              }
              return versionedUrn;
            }).collect(Collectors.toSet()), projectedAspects);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to batch get versioned entities: %s, projectedAspects: %s", versionedUrnStrs, projectedAspects),
            e);
      }
    }, MetricRegistry.name(this.getClass(), "batchGet"));
  }
}
