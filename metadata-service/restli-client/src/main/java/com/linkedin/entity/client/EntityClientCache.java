package com.linkedin.entity.client;

import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.linkedin.common.client.ClientCache;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Builder
public class EntityClientCache {
  @NonNull private EntityClientCacheConfig config;
  @NonNull private final ClientCache<Key, EnvelopedAspect, EntityClientCacheConfig> cache;
  @NonNull private BiFunction<Set<Urn>, Set<String>, Map<Urn, EntityResponse>> loadFunction;

  public EntityResponse getV2(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    return batchGetV2(Set.of(urn), aspectNames).get(urn);
  }

  public Map<Urn, EntityResponse> batchGetV2(
      @Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    final Map<Urn, EntityResponse> response;

    if (config.isEnabled()) {
      Set<Key> keys =
          urns.stream()
              .flatMap(
                  urn ->
                      aspectNames.stream().map(a -> Key.builder().urn(urn).aspectName(a).build()))
              .collect(Collectors.toSet());
      Map<Key, EnvelopedAspect> envelopedAspects = cache.getAll(keys);

      Set<EntityResponse> responses =
          envelopedAspects.entrySet().stream()
              .map(entry -> Pair.of(entry.getKey().getUrn(), entry.getValue()))
              .collect(
                  Collectors.groupingBy(
                      Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toSet())))
              .entrySet()
              .stream()
              .map(e -> toEntityResponse(e.getKey(), e.getValue()))
              .collect(Collectors.toSet());

      response =
          responses.stream().collect(Collectors.toMap(EntityResponse::getUrn, Function.identity()));
    } else {
      response = loadFunction.apply(urns, aspectNames);
    }

    return response;
  }

  private static EntityResponse toEntityResponse(
      Urn urn, Collection<EnvelopedAspect> envelopedAspects) {
    final EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(urnToEntityName(urn));
    response.setAspects(
        new EnvelopedAspectMap(
            envelopedAspects.stream()
                .collect(Collectors.toMap(EnvelopedAspect::getName, aspect -> aspect))));
    return response;
  }

  public static class EntityClientCacheBuilder {

    private EntityClientCacheBuilder cache(LoadingCache<Key, EnvelopedAspect> cache) {
      return this;
    }

    public EntityClientCache build(Class<?> metricClazz) {
      // estimate size
      Weigher<Key, EnvelopedAspect> weighByEstimatedSize =
          (key, value) -> value.getValue().data().toString().getBytes().length;

      // batch loads data from entity client (restli or java)
      Function<Iterable<? extends Key>, Map<Key, EnvelopedAspect>> loader =
          (Iterable<? extends Key> keys) -> {
            Map<String, Set<Key>> keysByEntity =
                StreamSupport.stream(keys.spliterator(), true)
                    .collect(Collectors.groupingBy(Key::getEntityName, Collectors.toSet()));

            Map<Key, EnvelopedAspect> results =
                keysByEntity.entrySet().stream()
                    .flatMap(
                        entry -> {
                          Set<Urn> urns =
                              entry.getValue().stream()
                                  .map(Key::getUrn)
                                  .collect(Collectors.toSet());
                          Set<String> aspects =
                              entry.getValue().stream()
                                  .map(Key::getAspectName)
                                  .collect(Collectors.toSet());
                          return loadFunction.apply(urns, aspects).entrySet().stream();
                        })
                    .flatMap(
                        resp ->
                            resp.getValue().getAspects().values().stream()
                                .map(
                                    envAspect -> {
                                      Key key =
                                          Key.builder()
                                              .urn(resp.getKey())
                                              .aspectName(envAspect.getName())
                                              .build();
                                      return Map.entry(key, envAspect);
                                    }))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return results;
          };

      // ideally the cache time comes from caching headers from service, but configuration driven
      // for now
      BiFunction<EntityClientCacheConfig, Key, Integer> ttlSeconds =
          (config, key) ->
              Optional.ofNullable(config.getEntityAspectTTLSeconds())
                  .orElse(Map.of())
                  .getOrDefault(key.getEntityName(), Map.of())
                  .getOrDefault(key.getAspectName(), config.getDefaultTTLSeconds());

      cache =
          ClientCache.<Key, EnvelopedAspect, EntityClientCacheConfig>builder()
              .weigher(weighByEstimatedSize)
              .config(config)
              .loadFunction(loader)
              .ttlSecondsFunction(ttlSeconds)
              .build(metricClazz);

      return new EntityClientCache(config, cache, loadFunction);
    }
  }

  @Data
  @Builder
  protected static class Key {
    private final Urn urn;
    private final String aspectName;

    public String getEntityName() {
      return urn.getEntityType();
    }
  }
}
