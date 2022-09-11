package com.linkedin.metadata.entity.validation;

import com.linkedin.data.template.RecordTemplate;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ValidationUtils {

  /**
   * Validates a {@link RecordTemplate} and throws {@link com.linkedin.restli.server.RestLiServiceException}
   * if validation fails.
   *
   * @param record record to be validated.
   */
  public static void validateOrThrow(RecordTemplate record) {
    RecordTemplateValidator.validate(record, validationResult -> {
      throw new ValidationException(
          String.format("Failed to validate record with class %s: %s",
          record.getClass().getName(),
          validationResult.getMessages().toString()));
    });
  }

  /**
   * Validates a {@link RecordTemplate} and logs a warning if validation fails.
   *
   * @param record record to be validated.ailure.
   */
  public static void validateOrWarn(RecordTemplate record) {
    RecordTemplateValidator.validate(record, validationResult -> {
      log.warn(String.format("Failed to validate record %s against its schema.", record));
    });
  }

  private ValidationUtils() {
  }
}