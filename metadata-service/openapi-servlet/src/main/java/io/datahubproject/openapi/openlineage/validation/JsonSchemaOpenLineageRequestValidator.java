package io.datahubproject.openapi.openlineage.validation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.ValidationMessage;
import io.datahubproject.openapi.openlineage.exception.InvalidOpenLineageEventException;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog.AttachmentPoint;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public final class JsonSchemaOpenLineageRequestValidator implements OpenLineageRequestValidator {
  private static final int MAX_ERRORS = 50;
  private static final Comparator<String> NULLABLE_STRINGS =
      Comparator.nullsFirst(String::compareTo);
  private static final Comparator<OpenLineageValidationError> ERROR_ORDER =
      Comparator.comparing(OpenLineageValidationError::path, NULLABLE_STRINGS)
          .thenComparing(OpenLineageValidationError::attachment, NULLABLE_STRINGS)
          .thenComparing(OpenLineageValidationError::facet, NULLABLE_STRINGS)
          .thenComparing(OpenLineageValidationError::rule, NULLABLE_STRINGS);
  private static final ObjectMapper STRICT_OBJECT_MAPPER =
      new ObjectMapper(
              JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private final OpenLineageSchemaCatalog schemas;

  public JsonSchemaOpenLineageRequestValidator(OpenLineageSchemaCatalog schemas) {
    this.schemas = schemas;
  }

  @Override
  public JsonNode validate(byte[] requestBody) throws InvalidOpenLineageEventException {
    JsonNode event = parse(requestBody);
    List<OpenLineageValidationError> errors = new ArrayList<>();
    addSchemaErrors(schemas.rootSchema(), envelopeForValidation(event), "$", null, null, errors);
    if (event.isObject()) {
      validateUriField((ObjectNode) event, "schemaURL", "$", null, null, errors);
    }

    walkFacetMaps(event, errors);
    reject(errors);
    return event;
  }

  private static JsonNode parse(byte[] requestBody) {
    if (requestBody == null || requestBody.length == 0) {
      throw invalidJson("required");
    }
    try {
      JsonNode event = STRICT_OBJECT_MAPPER.readTree(requestBody);
      if (event == null) {
        throw invalidJson("required");
      }
      return event;
    } catch (JsonProcessingException exception) {
      String message = exception.getOriginalMessage();
      String rule =
          message != null && message.startsWith("Duplicate field")
              ? "duplicateKey"
              : message != null && message.startsWith("Trailing token")
                  ? "trailingContent"
                  : "malformedJson";
      throw invalidJson(rule);
    } catch (java.io.IOException exception) {
      throw invalidJson("malformedJson");
    }
  }

  private static InvalidOpenLineageEventException invalidJson(String rule) {
    return new InvalidOpenLineageEventException(
        List.of(new OpenLineageValidationError("$", rule, null, null)));
  }

  private static JsonNode envelopeForValidation(JsonNode event) {
    if (!event.isObject()) {
      return event;
    }
    ObjectNode envelope = event.deepCopy();
    if (!envelope.has("schemaURL")) {
      envelope.put("schemaURL", OpenLineageSchemaCatalog.ROOT_SCHEMA_URI);
    }
    removeFacetMap(envelope.path("run"));
    removeFacetMap(envelope.path("job"));
    removeFacetMap(envelope.path("dataset"));
    removeDatasetFacetMaps(envelope.path("inputs"));
    removeDatasetFacetMaps(envelope.path("outputs"));
    return envelope;
  }

  private static void removeDatasetFacetMaps(JsonNode datasets) {
    if (!datasets.isArray()) {
      return;
    }
    for (JsonNode dataset : datasets) {
      removeFacetMap(dataset);
      if (dataset.isObject()) {
        ((ObjectNode) dataset).remove(List.of("inputFacets", "outputFacets"));
      }
    }
  }

  private static void removeFacetMap(JsonNode value) {
    if (value.isObject()) {
      ((ObjectNode) value).remove("facets");
    }
  }

  private void walkFacetMaps(JsonNode event, List<OpenLineageValidationError> errors) {
    visitFacetMap(event.path("run").path("facets"), AttachmentPoint.RUN, "$.run.facets", errors);
    visitFacetMap(event.path("job").path("facets"), AttachmentPoint.JOB, "$.job.facets", errors);
    visitDataset(event.path("dataset"), "$.dataset", errors);
    visitDatasets(event.path("inputs"), true, "$.inputs", errors);
    visitDatasets(event.path("outputs"), false, "$.outputs", errors);
  }

  private void visitDatasets(
      JsonNode datasets, boolean input, String path, List<OpenLineageValidationError> errors) {
    if (!datasets.isArray()) {
      return;
    }
    for (int index = 0; index < datasets.size(); index++) {
      JsonNode dataset = datasets.get(index);
      String datasetPath = path + "[" + index + "]";
      visitDataset(dataset, datasetPath, errors);
      AttachmentPoint attachment =
          input ? AttachmentPoint.INPUT_DATASET : AttachmentPoint.OUTPUT_DATASET;
      visitFacetMap(
          dataset.path(input ? "inputFacets" : "outputFacets"),
          attachment,
          datasetPath + (input ? ".inputFacets" : ".outputFacets"),
          errors);
      visitFacetMap(
          dataset.path(input ? "outputFacets" : "inputFacets"),
          attachment,
          datasetPath + (input ? ".outputFacets" : ".inputFacets"),
          errors,
          false);
    }
  }

  private void visitDataset(
      JsonNode dataset, String path, List<OpenLineageValidationError> errors) {
    visitFacetMap(dataset.path("facets"), AttachmentPoint.DATASET, path + ".facets", errors);
  }

  private void visitFacetMap(
      JsonNode facetMap,
      AttachmentPoint attachment,
      String path,
      List<OpenLineageValidationError> errors) {
    visitFacetMap(facetMap, attachment, path, errors, true);
  }

  private void visitFacetMap(
      JsonNode facetMap,
      AttachmentPoint attachment,
      String path,
      List<OpenLineageValidationError> errors,
      boolean allowStandardFacets) {
    if (facetMap.isMissingNode()) {
      return;
    }
    if (!facetMap.isObject()) {
      addError(errors, new OpenLineageValidationError(path, "type", null, attachment.name()));
      return;
    }
    List<String> keys = new ArrayList<>();
    facetMap.fieldNames().forEachRemaining(keys::add);
    keys.sort(String::compareTo);
    for (String key : keys) {
      JsonNode facet = facetMap.get(key);
      String facetPath = path + propertyPath(key);
      if (facet.isObject()) {
        validateUriField((ObjectNode) facet, "_producer", facetPath, key, attachment, errors);
        validateUriField((ObjectNode) facet, "_schemaURL", facetPath, key, attachment, errors);
      }
      Optional<OpenLineageSchemaCatalog.StandardFacetContract> standard =
          schemas.standardFacet(attachment, key);
      if (allowStandardFacets && standard.isPresent()) {
        validateStandardFacet(standard.get(), facet, facetPath, errors);
        continue;
      }
      if (schemas.isStandardFacetKey(key)) {
        addError(
            errors,
            new OpenLineageValidationError(facetPath, "attachment", key, attachment.name()));
        continue;
      }

      if (!facet.isObject()) {
        addError(errors, new OpenLineageValidationError(facetPath, "type", key, attachment.name()));
      }
    }
  }

  private void validateStandardFacet(
      OpenLineageSchemaCatalog.StandardFacetContract contract,
      JsonNode facet,
      String path,
      List<OpenLineageValidationError> errors) {
    JsonNode validationFacet = facet;
    if (facet.isObject()) {
      ObjectNode normalized = facet.deepCopy();
      if (!normalized.has("_producer")) {
        normalized.put("_producer", OpenLineageSchemaCatalog.ROOT_SCHEMA_URI);
      }
      if (!normalized.has("_schemaURL")) {
        normalized.put("_schemaURL", contract.schemaDocumentUri().toString());
      }
      normalizeLegacyStandardFacet(contract.attachment(), contract.key(), normalized);
      validationFacet = normalized;
    }
    addSchemaErrors(
        contract.schema(), validationFacet, path, contract.key(), contract.attachment(), errors);
  }

  private static void normalizeLegacyStandardFacet(
      AttachmentPoint attachment, String facetKey, ObjectNode facet) {
    if (attachment == AttachmentPoint.INPUT_DATASET
        && "dataQualityMetrics".equals(facetKey)
        && !facet.has("columnMetrics")
        && (facet.has("rowCount")
            || facet.has("bytes")
            || facet.has("fileCount")
            || facet.has("lastUpdated"))) {
      // OpenLineage 1-0-1 allowed aggregate-only metrics. Validate their consumed values while
      // supplying the empty column map required by the current bundled facet schema.
      facet.putObject("columnMetrics");
    }
  }

  private static void addSchemaErrors(
      JsonSchema schema,
      JsonNode instance,
      String path,
      String facet,
      AttachmentPoint attachment,
      List<OpenLineageValidationError> errors) {
    if (schema == null) {
      return;
    }
    for (ValidationMessage message : schema.validate(instance)) {
      String instancePath = message.getInstanceLocation().toString();
      String completePath = "$".equals(instancePath) ? path : path + instancePath.substring(1);
      addError(
          errors,
          new OpenLineageValidationError(
              completePath,
              message.getType(),
              facet,
              attachment == null ? null : attachment.name()));
    }
  }

  private static void validateUriField(
      ObjectNode object,
      String field,
      String path,
      String facet,
      AttachmentPoint attachment,
      List<OpenLineageValidationError> errors) {
    JsonNode value = object.get(field);
    if (value == null || value.isNull()) {
      return;
    }
    String fieldPath = path + "." + field;
    if (!value.isTextual()) {
      addError(
          errors,
          new OpenLineageValidationError(
              fieldPath, "type", facet, attachment == null ? null : attachment.name()));
      return;
    }
    try {
      if (!new URI(value.textValue()).isAbsolute()) {
        addError(
            errors,
            new OpenLineageValidationError(
                fieldPath, "format", facet, attachment == null ? null : attachment.name()));
      }
    } catch (URISyntaxException exception) {
      addError(
          errors,
          new OpenLineageValidationError(
              fieldPath, "format", facet, attachment == null ? null : attachment.name()));
    }
  }

  private static void addError(
      List<OpenLineageValidationError> errors, OpenLineageValidationError error) {
    int index = java.util.Collections.binarySearch(errors, error, ERROR_ORDER);
    if (index >= 0) {
      return;
    }
    int insertionPoint = -index - 1;
    if (insertionPoint >= MAX_ERRORS) {
      return;
    }
    errors.add(insertionPoint, error);
    if (errors.size() > MAX_ERRORS) {
      errors.remove(MAX_ERRORS);
    }
  }

  private static String propertyPath(String property) {
    return property.matches("[A-Za-z_][A-Za-z0-9_]*")
        ? "." + property
        : "['" + property.replace("'", "\\'") + "']";
  }

  private static void reject(List<OpenLineageValidationError> errors) {
    if (!errors.isEmpty()) {
      throw new InvalidOpenLineageEventException(List.copyOf(errors));
    }
  }
}
