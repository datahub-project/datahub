package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.UrnValidationFieldSpec;
import com.linkedin.metadata.models.annotation.UrnValidationAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UrnValidationUtil {
  public static final int URN_NUM_BYTES_LIMIT = 512;
  // Related to BrowsePathv2
  public static final String URN_DELIMITER_SEPARATOR = "␟";
  // https://datahubproject.io/docs/what/urn/#restrictions
  public static final Set<String> ILLEGAL_URN_CHARACTERS_PARENTHESES = Set.of("(", ")");
  // Commas are used as delimiters in tuple URNs, but not allowed in URN components
  public static final Set<String> ILLEGAL_URN_COMPONENT_DELIMITER = Set.of(",");

  private UrnValidationUtil() {}

  public static void validateUrn(
      @Nonnull EntityRegistry entityRegistry, @Nonnull final Urn urn, boolean strict) {
    EntityRegistryUrnValidator validator = new EntityRegistryUrnValidator(entityRegistry);
    validator.setCurrentEntitySpec(entityRegistry.getEntitySpec(urn.getEntityType()));
    RecordTemplateValidator.validate(
        EntityApiUtils.buildKeyAspect(entityRegistry, urn),
        validationResult -> {
          throw new IllegalArgumentException(
              "Invalid urn: " + urn + "\n Cause: " + validationResult.getMessages());
        },
        validator);

    if (urn.toString().trim().length() != urn.toString().length()) {
      throw new IllegalArgumentException(
          "Error: cannot provide an URN with leading or trailing whitespace");
    }
    if (!Constants.SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType())
        && URLEncoder.encode(urn.toString()).length() > URN_NUM_BYTES_LIMIT) {
      throw new IllegalArgumentException(
          "Error: cannot provide an URN longer than "
              + Integer.toString(URN_NUM_BYTES_LIMIT)
              + " bytes (when URL encoded)");
    }

    if (urn.toString().contains(URN_DELIMITER_SEPARATOR)) {
      throw new IllegalArgumentException(
          "Error: URN cannot contain " + URN_DELIMITER_SEPARATOR + " character");
    }

    // Check if this is a simple (non-tuple) URN containing commas
    if (urn.getEntityKey().getParts().size() == 1) {
      String part = urn.getEntityKey().getParts().get(0);
      if (part.contains(",")) {
        if (strict) {
          throw new IllegalArgumentException(
              String.format(
                  "Simple URN %s contains comma character which is not allowed in non-tuple URNs",
                  urn));
        } else {
          log.error(
              "Simple URN {} contains comma character which is not allowed in non-tuple URNs", urn);
        }
      }
    }

    // Validate all the URN parts
    List<String> illegalComponents = validateUrnComponents(urn);

    if (!illegalComponents.isEmpty()) {
      String message =
          String.format(
              "Illegal characters detected in URN %s component(s): %s", urn, illegalComponents);

      if (strict) {
        throw new IllegalArgumentException(message);
      } else {
        log.error(message);
      }
    }

    try {
      Urn.createFromString(urn.toString());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /** Validates all components of a URN and returns a list of any illegal components. */
  private static List<String> validateUrnComponents(Urn urn) {
    int totalParts = urn.getEntityKey().getParts().size();

    return urn.getEntityKey().getParts().stream()
        .flatMap(part -> processUrnPartRecursively(part, totalParts))
        .collect(Collectors.toList());
  }

  /** Recursively process URN parts with URL decoding and check for illegal characters */
  private static Stream<String> processUrnPartRecursively(String urnPart, int totalParts) {
    // If this part is a nested URN, don't check it directly for illegal characters
    if (urnPart.startsWith("urn:li:")) {
      return Stream.empty();
    }

    // If this part has encoded parentheses, consider it valid
    if (urnPart.contains("%28") || urnPart.contains("%29")) {
      return Stream.empty();
    }

    // Check for unencoded parentheses in any part
    if (ILLEGAL_URN_CHARACTERS_PARENTHESES.stream().anyMatch(c -> urnPart.contains(c))) {
      return Stream.of(urnPart);
    }

    // For tuple parts (URNs with multiple components), check for illegal commas within components
    if (totalParts > 1) {
      if (ILLEGAL_URN_COMPONENT_DELIMITER.stream().anyMatch(c -> urnPart.contains(c))) {
        return Stream.of(urnPart);
      }
    }

    // If we reach here, the part is valid
    return Stream.empty();
  }

  /**
   * Traverses a DataMap and finds all fields with UrnValidation annotations
   *
   * @param item The item to traverse
   * @param aspectSpec The AspectSpec containing UrnValidation field specifications
   * @return Set of UrnValidationEntry containing field paths, values and annotations
   */
  @Nonnull
  public static <T extends BatchItem> Set<UrnValidationEntry> findUrnValidationFields(
      @Nonnull T item, @Nonnull AspectSpec aspectSpec) {

    Set<UrnValidationEntry> result = new HashSet<>();
    Map<String, UrnValidationFieldSpec> urnValidationSpecs =
        aspectSpec.getUrnValidationFieldSpecMap();

    if (item.getRecordTemplate() != null && item.getRecordTemplate().data() != null) {
      // Traverse the DataMap recursively
      traverseDataMap(item.getRecordTemplate().data(), "", urnValidationSpecs, result);
    }

    return result;
  }

  /**
   * Traverses multiple DataMaps and finds all fields with UrnValidation annotations
   *
   * @param items Collection of items to traverse
   * @param aspectSpec The AspectSpec containing UrnValidation field specifications
   * @return Map of items to set of UrnValidationEntry containing field paths, values and
   *     annotations
   */
  public static <T extends BatchItem> Map<T, Set<UrnValidationEntry>> findUrnValidationFields(
      @Nonnull Collection<T> items, @Nonnull AspectSpec aspectSpec) {

    Map<T, Set<UrnValidationEntry>> result = new HashMap<>();

    for (T item : items) {
      if (item != null) {
        result.put(item, findUrnValidationFields(item, aspectSpec));
      }
    }

    return result;
  }

  private static void traverseDataMap(
      DataMap dataMap,
      String currentPath,
      Map<String, UrnValidationFieldSpec> urnValidationSpecs,
      Set<UrnValidationEntry> result) {

    for (String key : dataMap.keySet()) {
      // Standardize path construction to always start with "/"
      String fieldPath;
      if (currentPath.isEmpty()) {
        fieldPath = "/" + key;
      } else {
        fieldPath = currentPath + "/" + key;
      }
      Object value = dataMap.get(key);

      // Check if current field has UrnValidation annotation
      UrnValidationFieldSpec spec = urnValidationSpecs.get(fieldPath);
      if (spec != null) {
        if (value instanceof String) {
          result.add(
              new UrnValidationEntry(fieldPath, (String) value, spec.getUrnValidationAnnotation()));
        } else if (value instanceof DataList) {
          DataList list = (DataList) value;
          for (Object item : list) {
            if (item instanceof String) {
              result.add(
                  new UrnValidationEntry(
                      fieldPath, (String) item, spec.getUrnValidationAnnotation()));
            }
          }
        }
      }

      // Recursively traverse nested DataMaps
      if (value instanceof DataMap) {
        traverseDataMap((DataMap) value, fieldPath, urnValidationSpecs, result);
      }
    }
  }

  /** Container class for URN validation field information */
  @Value
  public static class UrnValidationEntry {
    String fieldPath;
    String urn;
    UrnValidationAnnotation annotation;
  }
}
