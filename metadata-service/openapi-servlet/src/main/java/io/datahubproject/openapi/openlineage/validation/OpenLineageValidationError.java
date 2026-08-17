package io.datahubproject.openapi.openlineage.validation;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record OpenLineageValidationError(
    String path, String rule, String facet, String attachment) {}
