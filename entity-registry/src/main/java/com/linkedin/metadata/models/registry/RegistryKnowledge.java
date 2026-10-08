package com.linkedin.metadata.models.registry;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The single place that decides whether data fits this version's entity registry.
 *
 * <p>During a zero-downtime upgrade rollback, version N-1 reads data written by N: rows, events and
 * requests naming entity types or aspects N-1 doesn't know, and known aspects that reference
 * entities of unknown types. N-1 works with its own schema: it skips what it doesn't know rather
 * than failing, and never stores or indexes it. Every such check goes through this class so that
 * malformed input, missing aspect names and urns nested in keys are treated the same everywhere.
 * Logging and metrics for skips live in {@code UnknownDataGuard} (metadata-utils).
 */
public final class RegistryKnowledge {

  private static final String URN_PREFIX = "urn:li:";

  private RegistryKnowledge() {}

  /**
   * Classifies an entity type and an optional aspect name. A null aspect name classifies the entity
   * type alone (e.g. a key-only proposal or a delete of the whole entity).
   */
  @Nonnull
  public static RegistryFit classify(
      @Nonnull final EntityRegistry registry,
      @Nullable final String entityType,
      @Nullable final String aspectName) {
    if (entityType == null || entityType.isEmpty()) {
      return RegistryFit.MALFORMED;
    }
    return registry
        .findEntitySpec(entityType)
        .map(
            entitySpec ->
                aspectName == null || entitySpec.getAspectSpec(aspectName) != null
                    ? RegistryFit.KNOWN
                    : RegistryFit.UNKNOWN_ASPECT)
        .orElse(RegistryFit.UNKNOWN_ENTITY_TYPE);
  }

  /** Classifies a urn string, which may be null or unparseable, and an optional aspect name. */
  @Nonnull
  public static RegistryFit classifyUrn(
      @Nonnull final EntityRegistry registry,
      @Nullable final String urn,
      @Nullable final String aspectName) {
    if (urn == null) {
      return RegistryFit.MALFORMED;
    }
    try {
      return classify(registry, Urn.createFromString(urn).getEntityType(), aspectName);
    } catch (URISyntaxException | IllegalArgumentException e) {
      return RegistryFit.MALFORMED;
    }
  }

  /** True when the entity type and aspect are unknown to the registry (not malformed). */
  public static boolean isUnknown(
      @Nonnull final EntityRegistry registry,
      @Nullable final String entityType,
      @Nullable final String aspectName) {
    return classify(registry, entityType, aspectName).isUnknown();
  }

  /**
   * True if the urn's entity type, or the entity type of any urn in its key (e.g. the monitored
   * entity in {@code urn:li:monitor:(urn:li:dataset:(...),id)}), is not in the registry. Use it for
   * references, where an entity that can't be resolved can't be shown or linked either.
   */
  public static boolean referencesUnknownEntityType(
      @Nonnull final EntityRegistry registry, @Nonnull final Urn urn) {
    if (registry.findEntitySpec(urn.getEntityType()).isEmpty()) {
      return true;
    }
    for (String part : urn.getEntityKey().getParts()) {
      if (part.startsWith(URN_PREFIX)) {
        try {
          if (referencesUnknownEntityType(registry, Urn.createFromString(part))) {
            return true;
          }
        } catch (URISyntaxException e) {
          // A key part that merely looks like an urn is not a reference.
        }
      }
    }
    return false;
  }

  /**
   * True when a stored aspect was written under a newer schema version than this version's, i.e. by
   * a newer version before a rollback. Such an aspect can legitimately hold values this version's
   * schema or annotations don't allow; anything else holding them is invalid data.
   */
  public static boolean isWrittenByNewerSchema(
      @Nullable final SystemMetadata systemMetadata, @Nullable final AspectSpec aspectSpec) {
    return systemMetadata != null
        && aspectSpec != null
        && systemMetadata.hasSchemaVersion()
        && systemMetadata.getSchemaVersion() > aspectSpec.getSchemaVersion();
  }
}
