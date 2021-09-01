package com.linkedin.metadata.resources;

import com.linkedin.common.urn.UrnValidator;
import com.linkedin.data.schema.validation.CoercionMode;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ResourceUtils {

  private static final ValidationOptions DEFAULT_VALIDATION_OPTIONS = new ValidationOptions(
      RequiredMode.CAN_BE_ABSENT_IF_HAS_DEFAULT,
      CoercionMode.NORMAL,
      UnrecognizedFieldMode.DISALLOW
  );
  private static final UrnValidator URN_VALIDATOR = new UrnValidator();

  /**
   * Validates a {@link RecordTemplate} and throws {@link com.linkedin.restli.server.RestLiServiceException}
   * if validation fails.
   *
   * @param record record to be validated.
   * @param status the status code to return to the client on failure.
   */
  public static void validateOrThrow(RecordTemplate record, HttpStatus status) {
    final ValidationResult result = ValidateDataAgainstSchema.validate(
        record,
        DEFAULT_VALIDATION_OPTIONS,
        URN_VALIDATOR);
    if (!result.isValid()) {
      throw new RestLiServiceException(status, result.getMessages().toString());
    }
  }


  /**
   * Validates a {@link RecordTemplate} and logs a warning if validation fails.
   *
   * @param record record to be validated.ailure.
   */
  public static void validateOrWarn(RecordTemplate record) {
    final ValidationResult result = ValidateDataAgainstSchema.validate(
        record,
        DEFAULT_VALIDATION_OPTIONS,
        URN_VALIDATOR);
    if (!result.isValid()) {
      log.warn(String.format("Failed to validate record %s against its schema.", record));
    }
  }

  private ResourceUtils() { }

}