package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValidationUtils {

  /**
   * Validates a {@link RecordTemplate} and throws {@link
   * com.linkedin.restli.server.RestLiServiceException} if validation fails.
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
      ChangeType changeType,
      EntityRegistry entityRegistry,
      EntitySpec entitySpec,
      AspectSpec aspectSpec,
      Urn urn,
      @Nullable RecordTemplate aspect,
      @Nonnull AspectRetriever aspectRetriever) {
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
        EntityUtils.buildKeyAspect(entityRegistry, urn), resultFunction, validator);

    if (aspect != null) {
      RecordTemplateValidator.validate(aspect, resultFunction, validator);

      for (AspectPayloadValidator aspectValidator :
          entityRegistry.getAspectPayloadValidators(
              changeType, entitySpec.getName(), aspectSpec.getName())) {
        try {
          aspectValidator.validateProposed(changeType, urn, aspectSpec, aspect, aspectRetriever);
        } catch (AspectValidationException e) {
          throw new IllegalArgumentException(
              "Failed to validate aspect due to: " + e.getMessage(), e);
        }
      }
    }
  }

  private ValidationUtils() {}
}
