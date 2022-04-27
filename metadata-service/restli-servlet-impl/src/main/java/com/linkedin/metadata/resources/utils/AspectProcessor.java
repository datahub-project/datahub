package com.linkedin.metadata.resources.utils;

import com.linkedin.data.DataComplex;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import java.util.List;
import java.util.ListIterator;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AspectProcessor {

  private AspectProcessor() { }

  /**
   *
   * Utility method that provides utility methods to remove fields from a given aspect based on it's aspect spec that
   * follows the following logic:
   *
   * 1. If field is optional and not part of an array → remove the field.
   * 2. If is a field that is part of an array (has an `*` in the path spec)
   *  → go up to the nearest array and remove the element.
   *  Extra → If array only has 1 element which is being deleted→ optional rules (if optional set null, otherwise delete)
   * 3. If field is non-optional and does not belong to an array delete if and only if aspect becomes empty.
   *
   * @param value       Value to be removed from Aspect
   * @param aspect      Aspect in which the value property exists
   * @param schema      {@link DataSchema} of the aspect being processed
   * @param aspectPath  Path within the aspect to where the value can be found.
   * @return  An updated version of the provided aspect without the provided value.
   */
  public static Aspect getAspectWithReferenceRemoved(String value, RecordTemplate aspect, DataSchema schema, PathSpec aspectPath) {
    try {
      final DataMap copy = aspect.copy().data();
      final DataComplex newValue = traversePath(value, schema, copy,
          aspectPath.getPathComponents(), 0, null);
      return new Aspect((DataMap) newValue);
    } catch (CloneNotSupportedException e) {
      return new Aspect();
    }
  }

  private static DataComplex traversePath(String value, DataSchema schema, DataComplex o, List<String> pathComponents,
      int index, Boolean optional)
      throws CloneNotSupportedException {

    final String subPath = pathComponents.get(index);

    // Processing an array
    if (subPath.equals("*")) {
      // Process each entry
      return processArray(value, (ArrayDataSchema) schema, (DataList) o, pathComponents, index, optional);
    } else { // Processing a map
      return processMap(value, (RecordDataSchema) schema, (DataMap) o, pathComponents, index, optional);
    }
  }

  private static DataComplex processMap(String value, RecordDataSchema spec, DataMap record, List<String> pathComponents,
      int index, Boolean isParentOptional)
      throws CloneNotSupportedException {
    // If in the last component of the path spec
    if (index == pathComponents.size() - 1) {
      // Can only remove if field is optional or if (parent is optional AND struct will be empty or just with optionals)
      boolean canDelete = fieldIsOptional(spec, pathComponents, index) || Boolean.TRUE.equals(isParentOptional) &&
          recordWillBeEmpty(record);

      if (canDelete) {
        final DataMap clone = record.clone();
        final Object found = clone.remove(pathComponents.get(index));
        if (found == null) {
          log.error(String.format("[DANGLING POINTER GC] Unable to find value %s in data map %s at path %s", value, clone,
              pathComponents.subList(0, index)));
        }
        return clone;
      } else {
        log.warn(String.format("[DANGLING POINTER GC] Can not remove a field %s that is non-optional!", spec.getName()));
      }
    } else {
      // else traverse further down the tree.
      final String key = pathComponents.get(index);
      final boolean optionalField = spec.getField(key).getOptional();
      // Check if key exists, this may not exist because you are in wrong branch of the tree (i.e: iterating for an array)
      if (record.containsKey(key)) {
        final boolean optional = Boolean.TRUE.equals(isParentOptional) || optionalField;
        final DataComplex result = traversePath(value, spec.getField(key).getType(), (DataComplex) record.get(key), pathComponents,
            index + 1, optional);

        if (!result.values().isEmpty()) {
          record.put(key, result);
        } else if (optional) {
          record.remove(key);
        } else if (result instanceof DataList) {
          // Only non-optional lists can be placed as default values
          record.put(key, result);
        } else { // if we modified the value but can not set it because the field is not optional, simply log the message.
          log.warn(String.format("[DANGLING POINTER GC] Can not remove a field that is non-optional "
              + "and not part of an array %s", spec.getField(key).getName()));
          return record;
        }
      }
    }

    return record;
  }

  private static boolean recordWillBeEmpty(DataMap record) {
    return record.size() == 1;
  }

  private static boolean fieldIsOptional(RecordDataSchema spec, List<String> pathComponents, int index) {
    return spec.getField(pathComponents.get(index)).getOptional();
  }

  private static DataComplex processArray(String value, ArrayDataSchema spec, DataList aspectList,
      List<String> pathComponents, int index, Boolean isParentOptional)
      throws CloneNotSupportedException {
    // If in the last component of the path spec
    if (index == pathComponents.size() - 1) {
      final DataList clone = aspectList.clone();
      final boolean found = clone.remove(value);
      if (!found) {
        log.error(String.format("Unable to find value %s in aspect list %s at path %s", value, aspectList,
            pathComponents.subList(0, index)));
      }
      return clone;
    } else { // else traverse further down the tree.
      final ListIterator<Object> it = aspectList.listIterator();
      while (it.hasNext()) {
        final Object aspect = it.next();
        final DataComplex result = traversePath(value, spec.getItems(), (DataComplex) aspect, pathComponents,
            index + 1, isParentOptional);
        if (!result.values().isEmpty()) {
          it.set(result);
        } else {
          it.remove();
        }
      }
    }

    return aspectList;
  }
}
