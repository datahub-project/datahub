package com.linkedin.metadata.resources;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.RecordTemplateValidator;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ResourceUtils {

  /**
   * Validates a {@link RecordTemplate} and throws {@link com.linkedin.restli.server.RestLiServiceException}
   * if validation fails.
   *
   * @param record record to be validated.
   * @param status the status code to return to the client on failure.
   */
  public static void validateOrThrow(RecordTemplate record, HttpStatus status) {
    RecordTemplateValidator.validate(record, validationResult -> {
      throw new RestLiServiceException(status, validationResult.getMessages().toString());
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

  private ResourceUtils() {
  }
}