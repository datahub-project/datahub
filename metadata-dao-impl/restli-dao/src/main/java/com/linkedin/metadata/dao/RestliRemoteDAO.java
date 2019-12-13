package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.cache.Cache;


/**
 * A rest.li implementation of {@link BaseRemoteDAO} by fetching data from remote snapshot resources directly.
 *
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 * @param <ASPECT_UNION> must be the aspect type used in {@code SNAPSHOT}
 * @param <URN> must be the URN type used in {@code SNAPSHOT}
 */
public class RestliRemoteDAO<SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseRemoteDAO<ASPECT_UNION, URN> {

  protected final Client _restliClient;
  protected final Optional<Cache<AspectKey, RecordTemplate>> _cache;

  public RestliRemoteDAO(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Client restliClient) {
    super(aspectUnionClass);

    ModelUtils.validateSnapshotAspect(snapshotClass, aspectUnionClass);
    _restliClient = restliClient;
    _cache = Optional.empty();
  }

  public RestliRemoteDAO(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Client restliClient, @Nonnull Cache<AspectKey, RecordTemplate> cache) {
    super(aspectUnionClass);

    ModelUtils.validateSnapshotAspect(snapshotClass, aspectUnionClass);
    _restliClient = restliClient;
    _cache = Optional.of(cache);
  }

  @Nonnull
  @Override
  public Map<AspectKey<URN, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
      @Nonnull Set<AspectKey<URN, ? extends RecordTemplate>> aspectKeys) {
    // TODO: Replace this with parallel restli requests
    return aspectKeys.stream().collect(Collectors.toMap(Function.identity(), this::getAspect));
  }

  @Nonnull
  @Override
  public Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull URN urn) {

    aspectClasses.forEach(this::checkValidAspect);

    final Set<AspectVersion> aspectVersions = QueryUtils.latestAspectVersions(aspectClasses);
    final List<RecordTemplate> cachedAspects = getCachedAspects(aspectVersions, urn);
    final Set<AspectVersion> uncachedAspectVersions = getUncachedAspectVersions(aspectVersions, cachedAspects);

    final List<RecordTemplate> aspects = new ArrayList<>(cachedAspects);
    if (uncachedAspectVersions.size() > 0) {
      final Request<SNAPSHOT> request = RequestBuilders.getBuilder(urn).getRequest(uncachedAspectVersions, urn);
      final List<RecordTemplate> uncachedAspects = ModelUtils.getAspectsFromSnapshot(getSnapshot(request));
      cacheAspects(urn, uncachedAspectVersions, uncachedAspects);
      aspects.addAll(uncachedAspects);
    }

    return aspectClasses.stream()
        .collect(Collectors.toMap(Function.identity(),
            aspectClass -> aspects.stream().filter(aspect -> aspect.getClass().equals(aspectClass)).findFirst()));
  }

  @Nonnull
  private <ASPECT extends RecordTemplate> Optional<ASPECT> getAspect(@Nonnull AspectKey<URN, ASPECT> aspectKey)
      throws IllegalArgumentException, RestliClientException {
    final Class<ASPECT> aspectClass = aspectKey.getAspectClass();
    checkValidAspect(aspectClass);

    final Optional<RecordTemplate> cachedAspect = getCachedAspect(aspectKey);
    if (cachedAspect.isPresent()) {
      return (Optional<ASPECT>) cachedAspect;
    }

    final URN urn = aspectKey.getUrn();
    final Request<SNAPSHOT> request =
        RequestBuilders.getBuilder(urn).getRequest(ModelUtils.getAspectName(aspectClass), urn, aspectKey.getVersion());

    final List<RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshot(getSnapshot(request));
    final Optional<ASPECT> aspect = (Optional<ASPECT>) aspects.stream().findFirst();
    aspect.ifPresent(a -> cacheAspect(aspectKey, a));
    return aspect;
  }

  @Nonnull
  private List<RecordTemplate> getCachedAspects(@Nonnull Set<AspectVersion> aspectVersions, @Nonnull URN urn) {

    final Set<AspectKey> aspectKeys = aspectVersions.stream()
        .map(av -> new AspectKey(ModelUtils.getAspectClass(av.getAspect()), urn, av.getVersion()))
        .collect(Collectors.toSet());
    return getCachedAspects(aspectKeys);
  }

  @Nonnull
  private Optional<RecordTemplate> getCachedAspect(@Nonnull AspectKey aspectKey) {
    return getCachedAspects(Collections.singleton(aspectKey)).stream().findFirst();
  }

  @Nonnull
  private List<RecordTemplate> getCachedAspects(@Nonnull Set<AspectKey> aspectKeys) {
    if (!_cache.isPresent()) {
      return Collections.emptyList();
    }
    return aspectKeys.stream().map(ak -> _cache.get().get(ak)).filter(Objects::nonNull).collect(Collectors.toList());
  }

  private void cacheAspects(@Nonnull URN urn, @Nonnull Set<AspectVersion> aspectVersions,
      @Nonnull List<RecordTemplate> aspects) {

    if (!_cache.isPresent()) {
      return;
    }

    aspects.forEach(aspect -> {
      final Class<? extends RecordTemplate> aspectClass = aspect.getClass();
      final AspectVersion aspectVersion = aspectVersions.stream()
          .filter(av -> av.getAspect().equals(ModelUtils.getAspectName(aspectClass)))
          .findFirst()
          .orElseThrow(() -> new IllegalStateException("API returned an unexpected aspect"));
      _cache.get().put(new AspectKey(aspectClass, urn, aspectVersion.getVersion()), aspect);
    });
  }

  private void cacheAspect(@Nonnull AspectKey aspectKey, @Nonnull RecordTemplate aspect) {
    _cache.ifPresent(cache -> cache.put(aspectKey, aspect));
  }

  @Nonnull
  private Set<AspectVersion> getUncachedAspectVersions(@Nonnull Set<AspectVersion> targetAspectVersions,
      @Nonnull List<RecordTemplate> cachedAspects) {

    final Set<String> cachedAspectNames =
        cachedAspects.stream().map(aspect -> ModelUtils.getAspectName(aspect.getClass())).collect(Collectors.toSet());
    return targetAspectVersions.stream()
        .filter(aspectVersion -> !cachedAspectNames.contains(aspectVersion.getAspect()))
        .collect(Collectors.toSet());
  }

  @Nonnull
  private SNAPSHOT getSnapshot(@Nonnull Request<SNAPSHOT> request) {
    try {
      return _restliClient.sendRequest(request).getResponse().getEntity();
    } catch (RemoteInvocationException e) {
      throw new RestliClientException(e);
    }
  }
}
