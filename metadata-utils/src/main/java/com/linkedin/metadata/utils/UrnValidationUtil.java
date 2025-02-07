package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.UrnValidationFieldSpec;
import com.linkedin.metadata.models.annotation.UrnValidationAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
  public static final Set<String> ILLEGAL_URN_COMPONENT_CHARACTERS = Set.of("(", ")");
  public static final Set<String> ILLEGAL_URN_TUPLE_CHARACTERS = Set.of(",");

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

    int totalParts = urn.getEntityKey().getParts().size();
    List<String> illegalComponents =
        urn.getEntityKey().getParts().stream()
            .flatMap(part -> processUrnPartRecursively(part, totalParts))
            .collect(Collectors.toList());

    if (!illegalComponents.isEmpty()) {
      String message =
          String.format(
              "Illegal `%s` characters detected in URN %s component(s): %s",
              ILLEGAL_URN_COMPONENT_CHARACTERS, urn, illegalComponents);

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

  /** Recursively process URN parts with URL decoding */
  private static Stream<String> processUrnPartRecursively(String urnPart, int totalParts) {
    String decodedPart =
        URLDecoder.decode(URLEncodingFixer.fixURLEncoding(urnPart), StandardCharsets.UTF_8);
    if (decodedPart.startsWith("urn:li:")) {
      // Recursively process nested URN after decoding
      int nestedParts = UrnUtils.getUrn(decodedPart).getEntityKey().getParts().size();
      return UrnUtils.getUrn(decodedPart).getEntityKey().getParts().stream()
          .flatMap(part -> processUrnPartRecursively(part, nestedParts));
    }
    if (totalParts > 1) {
      if (ILLEGAL_URN_TUPLE_CHARACTERS.stream().anyMatch(c -> urnPart.contains(c))) {
        return Stream.of(urnPart);
      }
    }
    if (ILLEGAL_URN_COMPONENT_CHARACTERS.stream().anyMatch(c -> urnPart.contains(c))) {
      return Stream.of(urnPart);
    }

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

  /**
   * Fixes malformed URL encoding by escaping unescaped % characters while preserving valid
   * percent-encoded sequences.
   */
  private static class URLEncodingFixer {
    /**
     * @param input The potentially malformed URL-encoded string
     * @return A string with proper URL encoding that can be safely decoded
     */
    public static String fixURLEncoding(String input) {
      if (input == null) {
        return null;
      }

      StringBuilder result = new StringBuilder(input.length() * 2);
      int i = 0;

      while (i < input.length()) {
        char currentChar = input.charAt(i);

        if (currentChar == '%') {
          if (i + 2 < input.length()) {
            // Check if the next two characters form a valid hex pair
            String hexPair = input.substring(i + 1, i + 3);
            if (isValidHexPair(hexPair)) {
              // This is a valid percent-encoded sequence, keep it as is
              result.append(currentChar);
            } else {
              // Invalid sequence, escape the % character
              result.append("%25");
            }
          } else {
            // % at the end of string, escape it
            result.append("%25");
          }
        } else {
          result.append(currentChar);
        }
        i++;
      }

      return result.toString();
    }

    private static boolean isValidHexPair(String pair) {
      return pair.matches("[0-9A-Fa-f]{2}");
    }
  }
}
