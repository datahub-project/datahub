package com.linkedin.metadata.entity;

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


/**
 * Utility class that encapsulates the logic on how to modify a {@link RecordTemplate} in place to remove a single value
 * following a concrete set of rules.
 *
 * It does this by a recursive tree traversal method, based on an aspect path provided for a given aspect.
 * This so that we don't have to scan the entire aspect for the value to be removed and then figure out how to apply
 * logical rules based on upstream optionality definitions.
 *
 * For more information see {@link #getAspectWithReferenceRemoved(String, RecordTemplate, DataSchema, PathSpec)}
 */
@Slf4j
public class DeleteEntityUtils {

  private DeleteEntityUtils() { }

  /**
   * Utility method that removes fields from a given aspect based on its aspect spec that follows the following logic:
   *
   * 1. If field is optional and not part of an array → remove the field.
   * 2. If is a field that is part of an array (has an `*` in the path spec)
   *  → go up to the nearest array and remove the element.
   *  Extra → If array only has 1 element which is being deleted→ optional rules (if optional set null, otherwise delete)
   * 3. If field is non-optional and does not belong to an array delete if and only if aspect becomes empty.
   *
   * @param value       Value to be removed from Aspect.
   * @param aspect      Aspect in which the value property exists.
   * @param schema      {@link DataSchema} of the aspect being processed.
   * @param aspectPath  Path within the aspect to where the value can be found.
   *
   * @return A deep copy of the aspect. Modified if the value was found and according to the logic specified above.
   * Otherwise, a copy of the original aspect is returned.
   */
  public static Aspect getAspectWithReferenceRemoved(String value, RecordTemplate aspect, DataSchema schema, PathSpec aspectPath) {
    try {
      final DataMap copy = aspect.copy().data();
      final DataComplex newValue = removeValueBasedOnPath(value, schema, copy, aspectPath.getPathComponents(), 0);
      if (newValue == null) {
        // If the new value is null, we should remove the aspect.
        return null;
      }
      return new Aspect((DataMap) newValue);
    } catch (CloneNotSupportedException e) {
      return new Aspect();
    }
  }

  /**
   * This method chooses which sub method to invoke based on the path being iterated on.
   *
   * @param value           The value to be removed from the data complex object. Merely propagated down in this method.
   * @param schema          The schema of the data complex being visited. Merely propagated down in this method.
   * @param o               The data complex instance with the actual data being visited.
   * @param pathComponents  The list of sub-strings representing the path from the root of the data complex to the value
   *                       to be removed.
   * @param index           The current index in the list of path components the method is meant to process.
   * @return  The updated DataComplex object without the value given as input.
   */
  private static DataComplex removeValueBasedOnPath(String value, DataSchema schema, DataComplex o, List<String> pathComponents,
      int index) {

    final String subPath = pathComponents.get(index);

    // Processing an array
    if (subPath.equals("*")) {
      // Process each entry
      return removeValueFromArray(value, (ArrayDataSchema) schema, (DataList) o, pathComponents, index);
    } else { // Processing a map
      return removeValueFromMap(value, (RecordDataSchema) schema, (DataMap) o, pathComponents, index);
    }
  }

  /**
   * This method is used to visit and remove values from DataMap instances if they are the leaf nodes of the original
   * data complex object.
   *
   * Note that this method has side effects and mutates the provided DataMap instance.
   *
   * @param value           The value to be removed from the data map object.
   * @param spec            The schema of the data complex being visited. Used to get information of the optionallity of
   *                        the data map being processed.
   * @param record          The data list instance with the actual data being visited.
   * @param pathComponents  The list of sub-strings representing the path from the root of the data complex to the value
   *                       to be removed.
   * @param index           The current index in the list of path components the method is meant to process.
   * @return  The updated DataComplex object without the value given as input.
   */
  private static DataComplex removeValueFromMap(String value, RecordDataSchema spec, DataMap record, List<String> pathComponents,
      int index) {
    // If in the last component of the path spec
    if (index == pathComponents.size() - 1) {
      boolean canDelete = spec.getField(pathComponents.get(index)).getOptional();
      boolean valueExistsInRecord = record.getOrDefault(pathComponents.get(index), "").equals(value);
      if (valueExistsInRecord) {
        if (canDelete) {
          record.remove(pathComponents.get(index));
        } else {
          // If the field is required, then we need to remove the entire record (if possible)
          return null;
        }
      } else {
        log.error("[Reference removal logic] Unable to find value {} in data map {} at path {}", value, record,
                pathComponents.subList(0, index));
      }
    } else {
      // else traverse further down the tree.
      final String key = pathComponents.get(index);
      final boolean optionalField = spec.getField(key).getOptional();
      // Check if key exists, this may not exist because you are in wrong branch of the tree (i.e: iterating for an array)
      if (record.containsKey(key)) {
        final DataComplex result = removeValueBasedOnPath(value, spec.getField(key).getType(), (DataComplex) record.get(key), pathComponents,
            index + 1);

        if (result == null) {
          if (optionalField) {
            record.remove(key);
          } else if (record.size() == 1) {
            return null;
          } else {
            // Not optional and not the only field, then this is a bad delete. Need to throw.
            throw new UnsupportedOperationException(
                String.format("Delete failed! Failed to field with name %s from DataMap. The field is required!", key));
          }
        } else {
          record.put(key, result);
        }
      }
    }
    return record;
  }

  /**
   * This method is used to visit and remove values from DataList instances if they are the leaf nodes of the original
   * data complex object.
   *
   * Note that this method has side effects and mutates the provided DataMap instance.
   *
   * @param value           The value to be removed from the data list object.
   * @param record          The data list instance with the actual data being visited.
   * @param pathComponents  The list of sub-strings representing the path from the root of the data complex to the value
   *                       to be removed.
   * @param index           The current index in the list of path components the method is meant to process.
   * @return  The updated DataComplex object without the value given as input.
   */
  private static DataComplex removeValueFromArray(String value, ArrayDataSchema record, DataList aspectList,
      List<String> pathComponents, int index) {
    // If in the last component of the path spec
    if (index == pathComponents.size() - 1) {
      final boolean found = aspectList.remove(value);
      if (!found) {
        log.error(String.format("Unable to find value %s in aspect list %s at path %s", value, aspectList,
            pathComponents.subList(0, index)));
      }
    } else { // else traverse further down the tree.
      final ListIterator<Object> it = aspectList.listIterator();
      while (it.hasNext()) {
        final Object aspect = it.next();
        final DataComplex result = removeValueBasedOnPath(value, record.getItems(), (DataComplex) aspect, pathComponents, index + 1);

        if (result == null) {
          it.remove();
        } else {
          it.set(result);
        }
      }
    }
    return aspectList;
  }
}
