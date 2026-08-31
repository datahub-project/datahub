package com.linkedin.metadata.aspect;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.models.registry.EmptyEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/** Responses can be cached based on application.yaml caching configuration for the EntityClient */
public interface CachingAspectRetriever extends AspectRetriever {

  CachingAspectRetriever EMPTY = new EmptyAspectRetriever();

  class EmptyAspectRetriever implements CachingAspectRetriever {
    @Nonnull
    @Override
    public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
        @Nonnull OperationFingerprint context, Set<Urn> urns, Set<String> aspectNames) {
      return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
        @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames) {
      return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public Map<Urn, Boolean> entityExists(@Nonnull OperationFingerprint context, Set<Urn> urns) {
      return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public EntityRegistry getEntityRegistry() {
      return EmptyEntityRegistry.EMPTY;
    }
  }
}
