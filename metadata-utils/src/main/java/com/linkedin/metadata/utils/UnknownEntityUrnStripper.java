package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.RegistryKnowledge;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Removes urns of entity types the registry does not know from an aspect, in place.
 *
 * <p>After a rollback, the older build can meet aspects it knows whose values reference entity
 * types only a newer build registered (e.g. a data product asset of a new type). The older build
 * must not store, index or serve such references, but must not fail the whole aspect either, so
 * this runs on every write and every read. For each such urn the smallest enclosing removable
 * element is dropped: the array element or map entry holding it, or the optional record field
 * holding it. A urn held by required fields or union members all the way up to the aspect root is
 * left in place: a write of that aspect is still rejected by validation, and a read serves it
 * unchanged (only logged), since dropping it would make read-modify-write callers treat the aspect
 * as missing and overwrite the newer version's data.
 *
 * <p>The walk follows the schema and only descends into parts of it that can hold an urn, so
 * aspects without urn fields cost one cached lookup.
 */
public final class UnknownEntityUrnStripper {

  private static final String URN_PREFIX = "urn:li:";

  // Whether a schema can hold an urn at any depth. Keyed by schema identity: schemas are shared
  // singletons, and their equals/hashCode are deep.
  private static final Map<SchemaKey, Boolean> HOLDS_URN = new ConcurrentHashMap<>();

  private UnknownEntityUrnStripper() {}

  /**
   * Removes the references in place. Callers report removals, since they know which entity the
   * aspect belongs to.
   *
   * @return the number of elements removed
   */
  public static int strip(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry entityRegistry) {
    final Walk walk = new Walk(entityRegistry, true);
    walk.prune(aspect.data(), aspect.schema());
    return walk.removed;
  }

  /**
   * True if the aspect holds a reference to an entity type the registry doesn't know, anywhere
   * (including ones {@link #strip} leaves in place because only required fields hold them). Does
   * not modify the aspect.
   */
  public static boolean referencesUnknownEntityType(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry entityRegistry) {
    final Walk walk = new Walk(entityRegistry, false);
    walk.prune(aspect.data(), aspect.schema());
    return walk.found;
  }

  /**
   * True when an aspect that fails validation can be attributed to a newer version: it was written
   * under a newer schema version, or it still references an entity type the registry doesn't know.
   * Callers skip such aspects and treat any other validation failure as a real error.
   */
  public static boolean isFromNewerVersion(
      @Nullable final RecordTemplate aspect,
      @Nullable final SystemMetadata systemMetadata,
      @Nullable final AspectSpec aspectSpec,
      @Nonnull final EntityRegistry entityRegistry) {
    return RegistryKnowledge.isWrittenByNewerSchema(systemMetadata, aspectSpec)
        || (aspect != null && referencesUnknownEntityType(aspect, entityRegistry));
  }

  private static final class Walk {
    private final EntityRegistry entityRegistry;
    private final boolean mutate;
    // Aspects repeat a few entity types many times (e.g. tags on every schema field).
    private final Map<String, Boolean> unknownByEntityType = new HashMap<>();
    private final Map<String, Boolean> unknownByNestedUrn = new HashMap<>();
    private int removed;
    private boolean found;

    private Walk(@Nonnull final EntityRegistry entityRegistry, final boolean mutate) {
      this.entityRegistry = entityRegistry;
      this.mutate = mutate;
    }

    /**
     * Removes unknown references below {@code value}. Returns true when {@code value} itself holds
     * one that only its container can remove (it is such a urn, or holds one through required
     * fields or a union member).
     */
    private boolean prune(@Nullable final Object value, @Nonnull final DataSchema schema) {
      if (value == null || !holdsUrn(schema)) {
        return false;
      }
      if (schema instanceof TyperefDataSchema typeref) {
        if (!isUrnTyperef(typeref)) {
          return prune(value, typeref.getRef());
        }
        final boolean unknown = value instanceof String urn && isUnknownEntityType(urn);
        found |= unknown;
        return unknown;
      }
      if (schema instanceof RecordDataSchema record && value instanceof DataMap map) {
        boolean containerMustGo = false;
        for (RecordDataSchema.Field field : record.getFields()) {
          if (prune(map.get(field.getName()), field.getType())) {
            if (!mutate) {
              continue;
            }
            if (field.getOptional()) {
              map.remove(field.getName());
              removed++;
            } else {
              containerMustGo = true;
            }
          }
        }
        return containerMustGo;
      }
      if (schema instanceof ArrayDataSchema array && value instanceof DataList list) {
        for (int i = list.size() - 1; i >= 0; i--) {
          if (prune(list.get(i), array.getItems()) && mutate) {
            list.remove(i);
            removed++;
          }
        }
        return false;
      }
      if (schema instanceof MapDataSchema mapSchema && value instanceof DataMap map) {
        for (String key : new ArrayList<>(map.keySet())) {
          if (prune(map.get(key), mapSchema.getValues()) && mutate) {
            map.remove(key);
            removed++;
          }
        }
        return false;
      }
      if (schema instanceof UnionDataSchema union && value instanceof DataMap member) {
        for (Map.Entry<String, Object> entry : member.entrySet()) {
          final DataSchema memberSchema = union.getTypeByMemberKey(entry.getKey());
          if (memberSchema != null && prune(entry.getValue(), memberSchema)) {
            return true;
          }
        }
      }
      return false;
    }

    private boolean isUnknownEntityType(@Nonnull final String urn) {
      final String entityType = RegistryKnowledge.entityTypeOf(urn);
      // Malformed urns (no entity type) are left for validation to reject.
      if (entityType == null) {
        return false;
      }
      if (unknownByEntityType.computeIfAbsent(
          entityType, type -> entityRegistry.findEntitySpec(type).isEmpty())) {
        return true;
      }
      // Urns nesting another urn in their key (e.g. a schema field or monitor of an entity type
      // only a newer version has) are parsed and checked whole; others skip the parse.
      return urn.indexOf(URN_PREFIX, URN_PREFIX.length()) > 0
          && unknownByNestedUrn.computeIfAbsent(urn, this::referencesUnknownEntityType);
    }

    private boolean referencesUnknownEntityType(@Nonnull final String urn) {
      try {
        return RegistryKnowledge.referencesUnknownEntityType(
            entityRegistry, Urn.createFromString(urn));
      } catch (URISyntaxException e) {
        // Malformed urns are left for validation to reject.
        return false;
      }
    }
  }

  private static boolean isUrnTyperef(@Nonnull final TyperefDataSchema typeref) {
    return typeref.getName().endsWith("Urn");
  }

  private static boolean holdsUrn(@Nonnull final DataSchema schema) {
    return HOLDS_URN.computeIfAbsent(
        new SchemaKey(schema),
        key -> holdsUrn(schema, Collections.newSetFromMap(new IdentityHashMap<>())));
  }

  private static boolean holdsUrn(
      @Nonnull final DataSchema schema, @Nonnull final Set<DataSchema> visited) {
    if (!visited.add(schema)) {
      return false;
    }
    if (schema instanceof TyperefDataSchema typeref) {
      return isUrnTyperef(typeref) || holdsUrn(typeref.getRef(), visited);
    }
    if (schema instanceof RecordDataSchema record) {
      return record.getFields().stream().anyMatch(f -> holdsUrn(f.getType(), visited));
    }
    if (schema instanceof ArrayDataSchema array) {
      return holdsUrn(array.getItems(), visited);
    }
    if (schema instanceof MapDataSchema map) {
      return holdsUrn(map.getValues(), visited);
    }
    if (schema instanceof UnionDataSchema union) {
      return union.getMembers().stream().anyMatch(m -> holdsUrn(m.getType(), visited));
    }
    return false;
  }

  /** Identity-based map key for a schema. */
  private record SchemaKey(DataSchema schema) {
    @Override
    public boolean equals(final Object other) {
      return other instanceof SchemaKey key && key.schema == schema;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(schema);
    }
  }
}
