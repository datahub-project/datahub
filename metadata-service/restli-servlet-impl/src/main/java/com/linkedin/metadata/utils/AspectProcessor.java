package com.linkedin.metadata.utils;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.models.AspectSpec;
import java.util.List;
import java.util.ListIterator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AspectProcessor {

  private AspectProcessor() { }

  public static Aspect removeAspect(String aspectName, PathSpec aspectPath, Aspect aspect, AspectSpec aspectSpec) throws CloneNotSupportedException {
    final DataMap copy =  aspect.copy().data();
    final Object newValue = traversePath(aspectName, aspectSpec.getPegasusSchema(), copy,
        aspectPath.getPathComponents(), 0);
    return new Aspect((DataMap) newValue);
  }

  private static Object traversePath(String value, DataSchema schema, Object o, List<String> pathComponents, int index)
      throws CloneNotSupportedException {

    final String subPath = pathComponents.get(index);

    // Processing an array
    if (subPath.equals("*")) {
      // Process each entry
      return processArray(value, (ArrayDataSchema) schema, (DataList) o, pathComponents, index);
    } else { // Processing a map
      return processMap(value, (RecordDataSchema) schema, (DataMap) o, pathComponents, index);
    }
  }

  private static Object processMap(String value, RecordDataSchema spec, DataMap record, List<String> pathComponents,
      int index)
      throws CloneNotSupportedException {
    // If in the last component of the path spec
    if (index == pathComponents.size() - 1) {
      final DataMap clone = record.clone();
      final Object found = clone.remove(pathComponents.get(index));
      if (found == null) {
        log.error(String.format("Unable to find value %s in aspect list %s at path %s", value, clone,
            pathComponents.subList(0, index)));
      } else if (clone.isEmpty()) {
        return null;
      }
      return clone;
    } else {
      // else traverse further down the tree.
      final String key = pathComponents.get(index);
      final boolean optional = spec.getField(key).getOptional();
      // Check if key exists, this may not exist because you are in wrong branch of the tree (i.e: iterating for an array)
      if (record.containsKey(key)) {
        final Object result = traversePath(value, spec.getField(key).getType(), record.get(key), pathComponents,
            index + 1);
        if (result != null) {
          record.put(key, result);
        } else if (optional) {
          record.remove(key);
        } else { // if we modified the value but can not set it because the field is not optional, simply log the message.
          log.warn(String.format("[DANGLING POINTER GC] Can not remove a field that is non-optional "
              + "and not part of an array %s", spec.getField(key).getName()));
          return record;
        }
      }
    }

    if (record.isEmpty()) {
      return null;
    }
    return record;
  }

  private static Object processArray(String value, ArrayDataSchema spec, DataList aspectList,
      List<String> pathComponents, int index)
      throws CloneNotSupportedException {
    // If in the last component of the path spec
    if (index == pathComponents.size() - 1) {
      final DataList clone = aspectList.clone();
      final boolean found = clone.remove(value);
      if (!found) {
        log.error(String.format("Unable to find value %s in aspect list %s at path %s", value, aspectList,
            pathComponents.subList(0, index)));
      } else if (clone.isEmpty()) {
        return null;
      }
      return clone;
    } else { // else traverse further down the tree.
      ListIterator<Object> it = aspectList.listIterator();
      while (it.hasNext()) {
        Object aspect = it.next();
        final Object result = traversePath(value, spec.getItems(), aspect, pathComponents, index + 1);
        if (result != null) {
          it.set(result);
        } else {
          it.remove();
        }
      }
    }

    if (aspectList.isEmpty()) {
      return null;
    }

    return aspectList;
  }
}
