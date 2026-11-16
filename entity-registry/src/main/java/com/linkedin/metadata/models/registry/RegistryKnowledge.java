package com.linkedin.metadata.models.registry;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
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

  /**
   * The entity type of {@code urn:li:<type>:<key>}, read without parsing the key; null when the
   * string isn't shaped like a urn (including an empty type or key), so callers leave it for
   * validation to reject. Cheap enough for hot paths that only need the type.
   */
  @Nullable
  public static String entityTypeOf(@Nullable final String urn) {
    if (urn == null || !urn.startsWith(URN_PREFIX)) {
      return null;
    }
    final int end = urn.indexOf(':', URN_PREFIX.length());
    return end <= URN_PREFIX.length() || end == urn.length() - 1
        ? null
        : urn.substring(URN_PREFIX.length(), end);
  }

  /** True when the entity type and aspect are unknown to the registry (not malformed). */
  public static boolean isUnknown(
      @Nonnull final EntityRegistry registry,
      @Nullable final String entityType,
      @Nullable final String aspectName) {
    return classify(registry, entityType, aspectName).isUnknown();
  }

  /**
   * True if the urn's entity type, or that of a urn its key holds in a urn-typed key field (e.g.
   * the monitored entity in {@code urn:li:monitor:(urn:li:dataset:(...),id)}), is not in the
   * registry. Use it for references, where an entity that can't be resolved can't be shown or
   * linked either.
   *
   * <p>Only key fields the key aspect types as a urn are references, as in key validation: a string
   * field that merely holds urn-shaped text (e.g. a dataset name) is not. A nested type the
   * registry doesn't know counts only when {@code newerVersionType} accepts it, so a nested type no
   * newer version registered (e.g. a typo) is left to validation, as an outer one is.
   */
  public static boolean referencesUnknownEntityType(
      @Nonnull final EntityRegistry registry,
      @Nonnull final Urn urn,
      @Nonnull final Predicate<String> newerVersionType) {
    return registry
        .findEntitySpec(urn.getEntityType())
        .map(entitySpec -> nestsUnknownEntityType(registry, urn, entitySpec, newerVersionType))
        .orElse(true);
  }

  private static boolean nestsUnknownEntityType(
      @Nonnull final EntityRegistry registry,
      @Nonnull final Urn urn,
      @Nonnull final EntitySpec entitySpec,
      @Nonnull final Predicate<String> newerVersionType) {
    final AspectSpec keyAspectSpec = entitySpec.getKeyAspectSpec();
    // Without a key schema there are no urn-typed key fields to hold a reference.
    if (keyAspectSpec == null) {
      return false;
    }
    final List<RecordDataSchema.Field> keyFields = keyAspectSpec.getPegasusSchema().getFields();
    final List<String> parts = urn.getEntityKey().getParts();
    // A key that doesn't match its schema is malformed; validation reports it.
    if (parts.size() != keyFields.size()) {
      return false;
    }
    for (int i = 0; i < parts.size(); i++) {
      if (!isUrnTyped(keyFields.get(i)) || !parts.get(i).startsWith(URN_PREFIX)) {
        continue;
      }
      final Urn nested;
      try {
        nested = Urn.createFromString(parts.get(i));
      } catch (URISyntaxException e) {
        // Malformed; validation reports it.
        continue;
      }
      final Optional<EntitySpec> nestedSpec = registry.findEntitySpec(nested.getEntityType());
      if (nestedSpec.isEmpty()
          ? newerVersionType.test(nested.getEntityType())
          : nestsUnknownEntityType(registry, nested, nestedSpec.get(), newerVersionType)) {
        return true;
      }
    }
    return false;
  }

  /**
   * A urn typeref (e.g. {@code Urn}, {@code DatasetUrn}), as {@code EntityRegistryUrnValidator}
   * reads it.
   */
  private static boolean isUrnTyped(@Nonnull final RecordDataSchema.Field field) {
    return field.getType() instanceof TyperefDataSchema
        && ((TyperefDataSchema) field.getType()).getName().endsWith("Urn");
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
