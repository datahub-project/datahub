package com.linkedin.metadata.search.semantic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Generates natural language text descriptions from entity documents for semantic search embedding
 * generation. Extracts only the searchable text fields as defined by @Searchable(fieldType: TEXT)
 * annotations in the aspect PDL files.
 */
@Slf4j
public class EntityTextGenerator {

  private final SearchableFieldExtractor _fieldExtractor;
  private final int _maxTextLength;

  public EntityTextGenerator(@Nonnull SearchableFieldExtractor fieldExtractor, int maxTextLength) {
    this._fieldExtractor = fieldExtractor;
    this._maxTextLength = maxTextLength;
  }

  /**
   * Generate a natural language text description from an entity document by extracting all
   * searchable text fields.
   *
   * @param entityType The type of entity (e.g., "dataset", "chart")
   * @param documentSource The source JSON document from the search index
   * @return A formatted text string suitable for embedding generation
   */
  @Nonnull
  public String generateText(@Nonnull String entityType, @Nonnull JsonNode documentSource) {
    List<SearchableTextField> textFields = _fieldExtractor.getSearchableTextFields(entityType);

    if (textFields.isEmpty()) {
      log.warn("No searchable text fields found for entity type: {}", entityType);
      return "";
    }

    StringBuilder textBuilder = new StringBuilder();
    int extractedFieldCount = 0;

    for (SearchableTextField field : textFields) {
      try {
        List<String> values = extractFieldValues(documentSource, field);
        if (!values.isEmpty()) {
          appendFieldValues(textBuilder, field, values);
          extractedFieldCount++;
        }
      } catch (Exception e) {
        log.debug(
            "Failed to extract field {} for entity type {}: {}",
            field.getFieldPath(),
            entityType,
            e.getMessage());
      }
    }

    String result = textBuilder.toString().trim();

    if (result.isEmpty()) {
      log.warn("Generated empty text for entity type: {}", entityType);
    } else {
      log.debug(
          "Generated text for {} with {} fields ({} chars)",
          entityType,
          extractedFieldCount,
          result.length());
    }

    if (result.length() > _maxTextLength) {
      result = truncateText(result, _maxTextLength);
      log.debug("Truncated text to {} characters", _maxTextLength);
    }

    return result;
  }

  /**
   * Extract values from a JSON document using a field path. Handles nested fields and arrays.
   *
   * @param documentSource The source JSON document
   * @param field The searchable text field definition
   * @return List of extracted string values
   */
  @Nonnull
  private List<String> extractFieldValues(
      @Nonnull JsonNode documentSource, @Nonnull SearchableTextField field) {
    List<String> values = new ArrayList<>();
    String fieldPath = field.getFieldPath();

    if (field.isNested()) {
      values.addAll(extractNestedFieldValues(documentSource, fieldPath));
    } else {
      String value = extractSimpleFieldValue(documentSource, fieldPath);
      if (value != null && !value.isEmpty()) {
        values.add(value);
      }
    }

    return values;
  }

  /**
   * Extract values from nested/array fields.
   *
   * @param documentSource The source JSON document
   * @param fieldPath The field path containing '*' for array elements
   * @return List of extracted string values
   */
  @Nonnull
  private List<String> extractNestedFieldValues(
      @Nonnull JsonNode documentSource, @Nonnull String fieldPath) {
    List<String> values = new ArrayList<>();

    String[] pathParts = fieldPath.split("/");
    JsonNode currentNode = documentSource;

    for (int i = 0; i < pathParts.length; i++) {
      String part = pathParts[i];

      if (part.isEmpty()) {
        continue;
      }

      if ("*".equals(part)) {
        if (currentNode != null && currentNode.isArray()) {
          ArrayNode arrayNode = (ArrayNode) currentNode;
          String remainingPath =
              String.join("/", java.util.Arrays.copyOfRange(pathParts, i + 1, pathParts.length));

          for (JsonNode element : arrayNode) {
            if (remainingPath.isEmpty()) {
              String value = extractNodeValue(element);
              if (value != null && !value.isEmpty()) {
                values.add(value);
              }
            } else {
              values.addAll(extractNestedFieldValues(element, remainingPath));
            }
          }

          return values;
        } else {
          return values;
        }
      } else {
        currentNode = currentNode != null ? currentNode.get(part) : null;
      }
    }

    String value = extractNodeValue(currentNode);
    if (value != null && !value.isEmpty()) {
      values.add(value);
    }

    return values;
  }

  /**
   * Extract a value from a simple (non-nested) field.
   *
   * @param documentSource The source JSON document
   * @param fieldPath The field path (no wildcards)
   * @return The extracted string value, or null if not found
   */
  @Nullable
  private String extractSimpleFieldValue(
      @Nonnull JsonNode documentSource, @Nonnull String fieldPath) {
    String[] pathParts = fieldPath.split("/");
    JsonNode currentNode = documentSource;

    for (String part : pathParts) {
      if (part.isEmpty()) {
        continue;
      }
      currentNode = currentNode != null ? currentNode.get(part) : null;
      if (currentNode == null) {
        return null;
      }
    }

    return extractNodeValue(currentNode);
  }

  /**
   * Extract string value from a JSON node, handling different node types.
   *
   * @param node The JSON node
   * @return The string value, or null if the node is null/missing
   */
  @Nullable
  private String extractNodeValue(@Nullable JsonNode node) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      return null;
    }

    if (node.isTextual()) {
      return node.asText();
    } else if (node.isNumber() || node.isBoolean()) {
      return node.asText();
    } else if (node.isObject() || node.isArray()) {
      return null;
    }

    return node.asText();
  }

  /**
   * Append field values to the text builder with formatting.
   *
   * @param textBuilder The string builder to append to
   * @param field The searchable text field
   * @param values The extracted values
   */
  private void appendFieldValues(
      @Nonnull StringBuilder textBuilder,
      @Nonnull SearchableTextField field,
      @Nonnull List<String> values) {
    if (values.isEmpty()) {
      return;
    }

    if (field.isNested()) {
      for (String value : values) {
        if (textBuilder.length() > 0) {
          textBuilder.append(" ");
        }
        textBuilder.append(value);
      }
    } else {
      if (textBuilder.length() > 0) {
        textBuilder.append(" ");
      }
      textBuilder.append(values.get(0));
    }
  }

  /**
   * Truncate text to a maximum length.
   *
   * @param text The text to truncate
   * @param maxLength The maximum length (returned string will not exceed this length)
   * @return The truncated text with "..." suffix, guaranteed to be at most maxLength characters
   */
  @Nonnull
  private String truncateText(@Nonnull String text, int maxLength) {
    if (text.length() <= maxLength) {
      return text;
    }

    // Reserve characters for the suffix
    String truncationIndicator = "...";
    int truncateAt = maxLength - truncationIndicator.length();
    return text.substring(0, truncateAt) + truncationIndicator;
  }
}
