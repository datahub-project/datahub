package io.datahubproject.openapi.openlineage.validation;

import com.fasterxml.jackson.databind.JsonNode;
import io.datahubproject.openapi.openlineage.exception.InvalidOpenLineageEventException;

public interface OpenLineageRequestValidator {
  JsonNode validate(byte[] requestBody) throws InvalidOpenLineageEventException;
}
