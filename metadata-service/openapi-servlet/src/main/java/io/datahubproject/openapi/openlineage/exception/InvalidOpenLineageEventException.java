package io.datahubproject.openapi.openlineage.exception;

import io.datahubproject.openapi.openlineage.validation.OpenLineageValidationError;
import java.util.List;
import lombok.Getter;

@Getter
public class InvalidOpenLineageEventException extends RuntimeException {
  private final String field;
  private final List<OpenLineageValidationError> validationErrors;

  public InvalidOpenLineageEventException(String message) {
    this(message, null);
  }

  public InvalidOpenLineageEventException(String message, String field) {
    super(message);
    this.field = field;
    this.validationErrors =
        field == null
            ? List.of()
            : List.of(new OpenLineageValidationError("$." + field, "invalid", null, null));
  }

  public InvalidOpenLineageEventException(List<OpenLineageValidationError> validationErrors) {
    super("Invalid OpenLineage event");
    this.validationErrors = List.copyOf(validationErrors);
    this.field = fieldFrom(this.validationErrors);
  }

  private static String fieldFrom(List<OpenLineageValidationError> errors) {
    if (errors.isEmpty()) {
      return null;
    }
    String path = errors.get(0).path();
    return path != null && path.startsWith("$.") && path.indexOf('.', 2) < 0
        ? path.substring(2)
        : null;
  }
}
