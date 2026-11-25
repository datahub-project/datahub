package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DEPRECATION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Deprecation;
import com.linkedin.common.FabricTypeNames;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * The one place that decides what an environment name is on the server: built-in, malformed, or a
 * custom name that is registered, deleted or unknown. Every check of an environment name that needs
 * more than its shape goes through {@link #resolve}, so a change to how registration is read, such
 * as caching, applies to every caller.
 *
 * <p>Built-in and malformed names are decided from the name alone. Only custom names are read, one
 * batched read for all of them.
 */
public final class FabricTypeLookup {

  public enum State {
    BUILT_IN,
    MALFORMED,
    /** Shaped like a custom name, but no customFabricType exists for it. */
    UNREGISTERED,
    DELETED,
    /** Registered and not deleted; see {@link Resolution#getDeprecation()} for deprecation. */
    REGISTERED
  }

  @Value
  public static class Resolution {
    @Nonnull State state;

    /** Set only for a registered type that is marked deprecated. */
    @Nullable Deprecation deprecation;
  }

  private static final Resolution BUILT_IN = new Resolution(State.BUILT_IN, null);
  private static final Resolution MALFORMED = new Resolution(State.MALFORMED, null);
  private static final Resolution UNREGISTERED = new Resolution(State.UNREGISTERED, null);
  private static final Resolution DELETED = new Resolution(State.DELETED, null);

  private FabricTypeLookup() {}

  /** The customFabricType urn that registers an environment name. */
  @Nonnull
  public static Urn urn(@Nonnull String name) {
    return UrnUtils.getUrn("urn:li:" + CUSTOM_FABRIC_TYPE_ENTITY_NAME + ":" + name);
  }

  /**
   * Resolves each name. Custom names are read through the caching retriever, then the missing ones
   * are re-read uncached, so a cached "missing" never rejects a type created since.
   *
   * @return a resolution for every name given
   */
  @Nonnull
  public static Map<String, Resolution> resolve(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<String> names,
      @Nonnull RetrieverContext retrieverContext) {
    Map<String, Resolution> resolved = new HashMap<>();
    Set<String> custom = new HashSet<>();
    for (String name : names) {
      switch (FabricTypeNames.kind(name)) {
        case BUILT_IN:
          resolved.put(name, BUILT_IN);
          break;
        case MALFORMED:
          resolved.put(name, MALFORMED);
          break;
        default:
          custom.add(name);
      }
    }
    if (custom.isEmpty()) {
      return resolved;
    }

    AspectRetriever uncached = retrieverContext.getAspectRetriever();
    AspectRetriever caching =
        retrieverContext instanceof io.datahubproject.metadata.context.RetrieverContext
            ? ((io.datahubproject.metadata.context.RetrieverContext) retrieverContext)
                .getCachingAspectRetriever()
            : uncached;

    Map<String, Resolution> registered = fetch(operationContext, custom, caching);
    Set<String> missing =
        custom.stream().filter(name -> !registered.containsKey(name)).collect(Collectors.toSet());
    if (!missing.isEmpty() && caching != uncached) {
      registered.putAll(fetch(operationContext, missing, uncached));
    }
    custom.forEach(name -> resolved.put(name, registered.getOrDefault(name, UNREGISTERED)));
    return resolved;
  }

  @Nonnull
  private static Map<String, Resolution> fetch(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Set<String> names,
      @Nonnull AspectRetriever retriever) {
    Map<Urn, String> namesByUrn =
        names.stream().collect(Collectors.toMap(FabricTypeLookup::urn, name -> name));
    Map<Urn, Map<String, Aspect>> aspects =
        retriever.getLatestAspectObjects(
            operationContext,
            namesByUrn.keySet(),
            Set.of(
                CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME, STATUS_ASPECT_NAME, DEPRECATION_ASPECT_NAME));

    Map<String, Resolution> found = new HashMap<>();
    aspects.forEach(
        (urn, byName) -> {
          if (byName == null || !byName.containsKey(CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME)) {
            return;
          }
          Aspect status = byName.get(STATUS_ASPECT_NAME);
          if (status != null && new Status(status.data()).isRemoved()) {
            found.put(namesByUrn.get(urn), DELETED);
            return;
          }
          Aspect deprecation = byName.get(DEPRECATION_ASPECT_NAME);
          Deprecation marked = deprecation == null ? null : new Deprecation(deprecation.data());
          found.put(
              namesByUrn.get(urn),
              new Resolution(
                  State.REGISTERED, marked != null && marked.isDeprecated() ? marked : null));
        });
    return found;
  }
}
