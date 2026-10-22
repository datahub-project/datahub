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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Keeps the stored references to entity types the registry doesn't know when an aspect is written.
 *
 * <p>After a rollback, reads drop references to entity types only a newer version registered, so a
 * client that writes back what it read would delete them. Instead, a write keeps every stored
 * reference to such a type that it couldn't have seen, at its stored position, and a client can't
 * delete one: one the request no longer holds is put back, even when the client cleared the list or
 * field that held it. Clearing a field readers did see (e.g. a record shown without its unknown
 * urn) stands, and deletes the references inside it with it. A reference to such a type that the
 * stored aspect doesn't hold is an addition, which the caller rejects; typos stay client errors.
 * Because kept references go back where they were stored, writing back what was read leaves the
 * aspect as stored.
 */
public final class UnknownReferenceMerger {

  private UnknownReferenceMerger() {}

  /**
   * @param preserved the stored references put back into the request
   * @param added the urns of unknown entity types the request holds that the stored aspect doesn't
   */
  public record MergeResult(int preserved, @Nonnull Set<String> added) {
    public static final MergeResult NONE = new MergeResult(0, Set.of());
  }

  /**
   * Puts the stored references to unknown entity types that {@code request} lacks back into it, in
   * place. Nothing is put back when the request adds such a reference; the caller rejects it.
   *
   * @param stored the stored aspect as written, before any read dropped references; null if none
   */
  @Nonnull
  public static MergeResult merge(
      @Nonnull final RecordTemplate request,
      @Nullable final RecordTemplate stored,
      @Nonnull final EntityRegistry entityRegistry) {
    final Set<String> storedUnknown =
        stored == null
            ? Set.of()
            : UnknownEntityUrnStripper.unknownUrns(stored.data(), stored.schema(), entityRegistry);
    final Set<String> added =
        new HashSet<>(
            UnknownEntityUrnStripper.unknownUrns(request.data(), request.schema(), entityRegistry));
    added.removeAll(storedUnknown);
    if (!added.isEmpty()) {
      return new MergeResult(0, added);
    }
    if (storedUnknown.isEmpty()) {
      return MergeResult.NONE;
    }
    final Merge merge = new Merge(entityRegistry);
    merge.merge(request.data(), stored.data(), request.schema());
    return new MergeResult(merge.preserved, Set.of());
  }

  private static final class Merge {
    private final EntityRegistry entityRegistry;
    private int preserved;

    private Merge(@Nonnull final EntityRegistry entityRegistry) {
      this.entityRegistry = entityRegistry;
    }

    /**
     * Puts the unknown references of {@code stored} that {@code request} lacks into it. Containers
     * are changed in place; the result is the value to hold where {@code request} was.
     */
    @Nullable
    private Object merge(
        @Nullable final Object request, @Nonnull final Object stored, @Nonnull DataSchema schema) {
      if (UnknownEntityUrnStripper.unknownUrns(stored, schema, entityRegistry).isEmpty()) {
        return request;
      }
      while (schema instanceof TyperefDataSchema typeref && !typeref.getName().endsWith("Urn")) {
        schema = typeref.getRef();
      }
      if (request == null) {
        // The client cleared what held them: keep only what it couldn't see. A list or map comes
        // back only if it held such elements, and anything else only if readers never saw it
        // (e.g. a record whose required urn is of an unknown type); clearing what they saw stands.
        if (schema instanceof ArrayDataSchema) {
          final DataList kept = (DataList) merge(new DataList(), stored, schema);
          return kept.isEmpty() ? null : kept;
        }
        if (schema instanceof MapDataSchema) {
          final DataMap kept = (DataMap) merge(new DataMap(), stored, schema);
          return kept.isEmpty() ? null : kept;
        }
        if (!UnknownEntityUrnStripper.wouldRemove(stored, schema, entityRegistry)) {
          return null;
        }
        preserved++;
        return UnknownEntityUrnStripper.copyOf(stored);
      }
      if (schema instanceof RecordDataSchema record
          && request instanceof DataMap requestMap
          && stored instanceof DataMap storedMap) {
        for (RecordDataSchema.Field field : record.getFields()) {
          final Object storedField = storedMap.get(field.getName());
          if (storedField == null) {
            continue;
          }
          final Object requestField = requestMap.get(field.getName());
          final Object merged = merge(requestField, storedField, field.getType());
          if (merged != requestField) {
            requestMap.put(field.getName(), merged);
          }
        }
        return request;
      }
      if (schema instanceof ArrayDataSchema array
          && request instanceof DataList requestList
          && stored instanceof DataList storedList) {
        mergeList(requestList, storedList, array.getItems());
        return request;
      }
      if (schema instanceof MapDataSchema map
          && request instanceof DataMap requestMap
          && stored instanceof DataMap storedMap) {
        mergeMap(requestMap, storedMap, map.getValues());
        return request;
      }
      if (schema instanceof UnionDataSchema union
          && request instanceof DataMap requestMember
          && stored instanceof DataMap storedMember) {
        for (Map.Entry<String, Object> entry : storedMember.entrySet()) {
          final DataSchema memberSchema = union.getTypeByMemberKey(entry.getKey());
          if (memberSchema != null && requestMember.containsKey(entry.getKey())) {
            final Object merged =
                merge(requestMember.get(entry.getKey()), entry.getValue(), memberSchema);
            requestMember.put(entry.getKey(), merged);
          }
        }
      }
      // A value the client replaced (e.g. a required urn it could see) stays as the client set it.
      return request;
    }

    private void mergeList(
        @Nonnull final DataList request,
        @Nonnull final DataList stored,
        @Nonnull final DataSchema items) {
      for (int i = 0; i < stored.size(); i++) {
        final Object element = stored.get(i);
        if (UnknownEntityUrnStripper.unknownUrns(element, items, entityRegistry).isEmpty()) {
          continue;
        }
        if (UnknownEntityUrnStripper.wouldRemove(element, items, entityRegistry)) {
          // Readers never saw it, so the client can't have meant to remove it: put it back at its
          // stored position.
          if (!request.contains(element)) {
            request.add(Math.min(i, request.size()), UnknownEntityUrnStripper.copyOf(element));
            preserved++;
          }
        } else {
          // Readers saw it without its unknown references; if the client kept it, keep them too.
          final int index =
              request.indexOf(
                  UnknownEntityUrnStripper.strippedCopy(element, items, entityRegistry));
          if (index >= 0) {
            request.set(index, merge(request.get(index), element, items));
          }
        }
      }
    }

    private void mergeMap(
        @Nonnull final DataMap request,
        @Nonnull final DataMap stored,
        @Nonnull final DataSchema values) {
      for (Map.Entry<String, Object> entry : stored.entrySet()) {
        final Object value = entry.getValue();
        if (UnknownEntityUrnStripper.unknownUrns(value, values, entityRegistry).isEmpty()) {
          continue;
        }
        if (UnknownEntityUrnStripper.wouldRemove(value, values, entityRegistry)) {
          if (!request.containsKey(entry.getKey())) {
            request.put(entry.getKey(), UnknownEntityUrnStripper.copyOf(value));
            preserved++;
          }
        } else if (request.containsKey(entry.getKey())) {
          request.put(entry.getKey(), merge(request.get(entry.getKey()), value, values));
        }
      }
    }
  }
}
