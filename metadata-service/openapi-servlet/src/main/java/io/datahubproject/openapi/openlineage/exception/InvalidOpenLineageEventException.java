package io.datahubproject.openapi.openlineage.exception;

import io.datahubproject.openapi.openlineage.validation.OpenLineageValidationError;
import java.util.List;
import lombok.Getter;

@Getter
public class InvalidOpenLineageEventException extends RuntimeException {
  private final List<OpenLineageValidationError> validationErrors;

  public InvalidOpenLineageEventException(List<OpenLineageValidationError> validationErrors) {
    super("Invalid OpenLineage event");
    this.validationErrors = List.copyOf(validationErrors);
  }
}
