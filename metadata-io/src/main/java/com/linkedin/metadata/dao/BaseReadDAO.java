package com.linkedin.metadata.dao;

import com.datahub.util.ModelUtils;
import com.datahub.util.exception.InvalidMetadataType;
import com.datahub.util.validator.AspectValidator;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public abstract class BaseReadDAO<ASPECT_UNION extends UnionTemplate, URN extends Urn> {

  public static final long FIRST_VERSION = 0;
  public static final long LATEST_VERSION = 0;

  // A set of pre-computed valid metadata types
  private final Set<Class<? extends RecordTemplate>> _validMetadataAspects;

  public BaseReadDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    AspectValidator.validateAspectUnionSchema(aspectUnionClass);

    _validMetadataAspects = ModelUtils.getValidAspectTypes(aspectUnionClass);
  }

  public BaseReadDAO(@Nonnull Set<Class<? extends RecordTemplate>> aspects) {
    _validMetadataAspects = aspects;
  }

  /**
   * Batch retrieves metadata aspects using multiple {@link AspectKey}s.
   *
   * @param keys set of keys for the metadata to retrieve
   * @return a mapping of given keys to the corresponding metadata aspect.
   */
  @Nonnull
  public abstract Map<AspectKey<URN, ? extends RecordTemplate>, Optional<? extends RecordTemplate>>
      get(@Nonnull Set<AspectKey<URN, ? extends RecordTemplate>> keys);

  /** Similar to {@link #get(Set)} but only using only one {@link AspectKey}. */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<ASPECT> get(@Nonnull AspectKey<URN, ASPECT> key) {
    return (Optional<ASPECT>) get(Collections.singleton(key)).get(key);
  }

  /**
   * Similar to {@link #get(AspectKey)} but with each component of the key broken out as arguments.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<ASPECT> get(
      @Nonnull Class<ASPECT> aspectClass, @Nonnull URN urn, long version) {
    return get(new AspectKey<>(aspectClass, urn, version));
  }

  /** Similar to {@link #get(Class, Urn, long)} but always retrieves the latest version. */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<ASPECT> get(
      @Nonnull Class<ASPECT> aspectClass, @Nonnull URN urn) {
    return get(aspectClass, urn, LATEST_VERSION);
  }

  /**
   * Similar to {@link #get(Class, Urn)} but retrieves multiple aspects latest versions associated
   * with multiple URNs.
   *
   * <p>The returned {@link Map} contains all the .
   */
  @Nonnull
  public Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> get(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull Set<URN> urns) {

    final Set<AspectKey<URN, ? extends RecordTemplate>> keys = new HashSet<>();
    for (URN urn : urns) {
      for (Class<? extends RecordTemplate> aspect : aspectClasses) {
        keys.add(new AspectKey<>(aspect, urn, LATEST_VERSION));
      }
    }

    final Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>>
        results = new HashMap<>();
    get(keys)
        .entrySet()
        .forEach(
            entry -> {
              final AspectKey<URN, ? extends RecordTemplate> key = entry.getKey();
              final URN urn = key.getUrn();
              results.putIfAbsent(urn, new HashMap<>());
              results.get(urn).put(key.getAspectClass(), entry.getValue());
            });

    return results;
  }

  /** Similar to {@link #get(Set, Set)} but only for one URN. */
  @Nonnull
  public Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull URN urn) {

    Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> results =
        get(aspectClasses, Collections.singleton(urn));
    if (!results.containsKey(urn)) {
      throw new IllegalStateException("Results should contain " + urn);
    }

    return results.get(urn);
  }

  /** Similar to {@link #get(Set, Set)} but only for one aspect. */
  @Nonnull
  public <ASPECT extends RecordTemplate> Map<URN, Optional<ASPECT>> get(
      @Nonnull Class<ASPECT> aspectClass, @Nonnull Set<URN> urns) {

    return get(Collections.singleton(aspectClass), urns).entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> (Optional<ASPECT>) entry.getValue().get(aspectClass)));
  }

  protected void checkValidAspect(@Nonnull Class<? extends RecordTemplate> aspectClass) {
    if (!_validMetadataAspects.contains(aspectClass)) {
      throw new InvalidMetadataType(aspectClass + " is not a supported metadata aspect type");
    }
  }

  protected void checkValidAspects(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    aspectClasses.forEach(aspectClass -> checkValidAspect(aspectClass));
  }
}
