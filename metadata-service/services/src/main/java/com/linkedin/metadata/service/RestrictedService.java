package com.linkedin.metadata.service;

import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.secret.SecretService;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class RestrictedService {
  public static final String RESTRICTED_ENTITY_TYPE = "restricted";

  private final SecretService secretService;
  private final EntityRegistry entityRegistry;

  public RestrictedService(
      @Nonnull SecretService secretService, @Nonnull EntityRegistry entityRegistry) {
    this.secretService = secretService;
    this.entityRegistry = entityRegistry;
  }

  public Urn encryptRestrictedUrn(@Nonnull final Urn entityUrn) {
    final String encryptedEntityUrn = this.secretService.encrypt(entityUrn.toString());
    try {
      return new Urn(RESTRICTED_ENTITY_TYPE, encryptedEntityUrn);
    } catch (Exception e) {
      throw new RuntimeException("Error when creating restricted entity urn", e);
    }
  }

  public Urn decryptRestrictedUrn(@Nonnull final Urn restrictedUrn) {
    final String encryptedUrn = restrictedUrn.getId();
    return UrnUtils.getUrn(this.secretService.decrypt(encryptedUrn));
  }

  public LineageSearchResult encryptRestricted(
      @Nonnull Function<Urn, Boolean> viewFilter, @Nonnull LineageSearchResult result) {
    return result.setEntities(encryptRestricted(viewFilter, result.getEntities(), new HashMap<>()));
  }

  public LineageScrollResult encryptRestricted(
      @Nonnull Function<Urn, Boolean> viewFilter, @Nonnull LineageScrollResult result) {
    return result.setEntities(encryptRestricted(viewFilter, result.getEntities(), new HashMap<>()));
  }

  private LineageSearchEntityArray encryptRestricted(
      @Nonnull Function<Urn, Boolean> viewFilter,
      @Nonnull LineageSearchEntityArray array,
      Map<Urn, Urn> cache) {
    for (LineageSearchEntity entity : array) {
      Urn entityUrn = entity.getEntity();
      entity.setEntity(getCachedRestrictedUrn(viewFilter, entityUrn, cache));
      entity.setPaths(getCachedRestrictedUrn(viewFilter, entity.getPaths(), cache));
    }
    return array;
  }

  private Urn getCachedRestrictedUrn(
      @Nonnull Function<Urn, Boolean> viewFilter,
      @Nonnull Urn entityUrn,
      @Nonnull Map<Urn, Urn> cache) {
    return cache.computeIfAbsent(
        entityUrn,
        urn -> {
          if (!viewFilter.apply(urn)) {
            return encryptRestrictedUrn(urn);
          }

          return urn;
        });
  }

  private UrnArrayArray getCachedRestrictedUrn(
      @Nonnull Function<Urn, Boolean> viewFilter, UrnArrayArray array, Map<Urn, Urn> cache) {
    return array.stream()
        .map(
            urnArray ->
                urnArray.stream()
                    .map(urn -> getCachedRestrictedUrn(viewFilter, urn, cache))
                    .collect(Collectors.toCollection(UrnArray::new)))
        .collect(Collectors.toCollection(UrnArrayArray::new));
  }
}
