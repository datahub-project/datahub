package com.linkedin.metadata.utils;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
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
 * left in place, so validation still rejects that one aspect.
 *
 * <p>The walk follows the schema and only descends into parts of it that can hold an urn, so
 * aspects without urn fields cost one cached lookup.
 */
public final class UnknownEntityUrnStripper {

  private static final UnknownDataGuard UNKNOWN_REFERENCES =
      UnknownDataGuard.forSite(UnknownEntityUrnStripper.class, "references in aspect");

  private static final String URN_PREFIX = "urn:li:";

  // Whether a schema can hold an urn at any depth. Keyed by schema identity: schemas are shared
  // singletons, and their equals/hashCode are deep.
  private static final Map<SchemaKey, Boolean> HOLDS_URN = new ConcurrentHashMap<>();

  private UnknownEntityUrnStripper() {}

  /**
   * @return the number of elements removed
   */
  public static int strip(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry entityRegistry) {
    final Walk walk = new Walk(entityRegistry);
    walk.prune(aspect.data(), aspect.schema());
    if (walk.removed > 0) {
      UNKNOWN_REFERENCES.skippedBecause(
          Optional.empty(),
          aspect.schema().getName(),
          walk.removed + " reference(s) to entity types not in the entity registry removed",
          aspect.schema().getName());
    }
    return walk.removed;
  }

  private static final class Walk {
    private final EntityRegistry entityRegistry;
    // Aspects repeat a few entity types many times (e.g. tags on every schema field).
    private final Map<String, Boolean> unknownByEntityType = new HashMap<>();
    private int removed;

    private Walk(@Nonnull final EntityRegistry entityRegistry) {
      this.entityRegistry = entityRegistry;
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
        return isUrnTyperef(typeref)
            ? value instanceof String urn && isUnknownEntityType(urn)
            : prune(value, typeref.getRef());
      }
      if (schema instanceof RecordDataSchema record && value instanceof DataMap map) {
        boolean containerMustGo = false;
        for (RecordDataSchema.Field field : record.getFields()) {
          if (prune(map.get(field.getName()), field.getType())) {
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
          if (prune(list.get(i), array.getItems())) {
            list.remove(i);
            removed++;
          }
        }
        return false;
      }
      if (schema instanceof MapDataSchema mapSchema && value instanceof DataMap map) {
        for (String key : new ArrayList<>(map.keySet())) {
          if (prune(map.get(key), mapSchema.getValues())) {
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
      final String entityType = entityTypeOf(urn);
      // Malformed urns (no entity type) are left for validation to reject.
      return entityType != null
          && unknownByEntityType.computeIfAbsent(
              entityType, type -> entityRegistry.findEntitySpec(type).isEmpty());
    }
  }

  /** The entity type of {@code urn:li:<type>:<key>}, without parsing the key; null if malformed. */
  @Nullable
  private static String entityTypeOf(@Nonnull final String urn) {
    if (!urn.startsWith(URN_PREFIX)) {
      return null;
    }
    final int end = urn.indexOf(':', URN_PREFIX.length());
    return end <= URN_PREFIX.length() ? null : urn.substring(URN_PREFIX.length(), end);
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
