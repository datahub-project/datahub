package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.entity.EntityApiUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValidationApiUtils {
  public static final String STRICT_URN_VALIDATION_ENABLED = "STRICT_URN_VALIDATION_ENABLED";
  public static final int URN_NUM_BYTES_LIMIT = 512;
  // Related to BrowsePathv2
  public static final String URN_DELIMITER_SEPARATOR = "␟";
  // https://datahubproject.io/docs/what/urn/#restrictions
  public static final Set<String> ILLEGAL_URN_COMPONENT_CHARACTERS = Set.of("(", ")");
  public static final Set<String> ILLEGAL_URN_TUPLE_CHARACTERS = Set.of(",");

  /**
   * Validates a {@link RecordTemplate} and throws {@link ValidationException} if validation fails.
   *
   * @param record record to be validated.
   */
  public static void validateOrThrow(RecordTemplate record) {
    RecordTemplateValidator.validate(
        record,
        validationResult -> {
          throw new ValidationException(
              String.format(
                  "Failed to validate record with class %s: %s",
                  record.getClass().getName(), validationResult.getMessages().toString()));
        });
  }

  public static void validateUrn(@Nonnull EntityRegistry entityRegistry, @Nonnull final Urn urn) {
    validateUrn(
        entityRegistry,
        urn,
        Boolean.TRUE.equals(
            Boolean.parseBoolean(
                System.getenv().getOrDefault(STRICT_URN_VALIDATION_ENABLED, "false"))));
  }

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
   * Validates a {@link RecordTemplate} and logs a warning if validation fails.
   *
   * @param record record to be validated.ailure.
   */
  public static void validateOrWarn(RecordTemplate record) {
    RecordTemplateValidator.validate(
        record,
        validationResult -> {
          log.warn(String.format("Failed to validate record %s against its schema.", record));
        });
  }

  public static AspectSpec validate(EntitySpec entitySpec, String aspectName) {
    if (aspectName == null || aspectName.isEmpty()) {
      throw new UnsupportedOperationException(
          "Aspect name is required for create and update operations");
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);

    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format("Unknown aspect %s for entity %s", aspectName, entitySpec.getName()));
    }

    return aspectSpec;
  }

  public static void validateRecordTemplate(
      EntitySpec entitySpec,
      Urn urn,
      @Nullable RecordTemplate aspect,
      @Nonnull AspectRetriever aspectRetriever) {
    EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();
    EntityRegistryUrnValidator validator = new EntityRegistryUrnValidator(entityRegistry);
    validator.setCurrentEntitySpec(entitySpec);
    Consumer<ValidationResult> resultFunction =
        validationResult -> {
          throw new IllegalArgumentException(
              "Invalid format for aspect: "
                  + entitySpec.getName()
                  + "\n Cause: "
                  + validationResult.getMessages());
        };

    RecordTemplateValidator.validate(
        EntityApiUtils.buildKeyAspect(entityRegistry, urn), resultFunction, validator);

    if (aspect != null) {
      RecordTemplateValidator.validate(aspect, resultFunction, validator);
    }
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
