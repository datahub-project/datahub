package io.datahubproject.openapi.openlineage.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonMetaSchema;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.PathType;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.resource.DisallowSchemaLoader;
import com.networknt.schema.resource.MapSchemaLoader;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog.AttachmentPoint;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public final class OpenLineageSchemaCatalog {
  static final String ROOT_SCHEMA_URI = "https://openlineage.io/spec/2-0-2/OpenLineage.json";
  private static final String MANIFEST_RESOURCE = "openlineage/schemas/1.45.0/manifest.json";

  public record StandardFacetContract(
      AttachmentPoint attachment,
      String key,
      URI schemaDocumentUri,
      String definition,
      JsonSchema schema) {}

  private record FacetKey(AttachmentPoint attachment, String key) {}

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final SchemaValidatorsConfig validatorConfig;
  private final JsonSchemaFactory schemaFactory;
  private final Map<String, URI> schemaDocumentUrisByName;
  private final JsonSchema rootSchema;
  private final Map<FacetKey, StandardFacetContract> standardFacets;
  private final Set<String> standardFacetKeys;

  public OpenLineageSchemaCatalog() {
    Map<String, String> schemaDocuments = loadSchemas();
    JsonMetaSchema metaSchema = JsonMetaSchema.getV202012();
    schemaFactory =
        JsonSchemaFactory.builder()
            .defaultMetaSchemaIri(metaSchema.getIri())
            .metaSchema(metaSchema)
            .schemaLoaders(
                loaders ->
                    loaders
                        .add(new MapSchemaLoader(schemaDocuments))
                        .add(DisallowSchemaLoader.getInstance()))
            .build();
    validatorConfig =
        SchemaValidatorsConfig.builder()
            .formatAssertionsEnabled(false)
            .failFast(false)
            .pathType(PathType.JSON_PATH)
            .preloadJsonSchema(true)
            .build();
    schemaDocumentUrisByName = indexSchemaDocuments(schemaDocuments.keySet());

    rootSchema = schema(ROOT_SCHEMA_URI);

    Map<FacetKey, StandardFacetContract> standards = new LinkedHashMap<>();
    addRunFacets(standards);
    addJobFacets(standards);
    addDatasetFacets(standards);
    addInputFacets(standards);
    addOutputFacets(standards);
    standardFacets = Map.copyOf(standards);
    standardFacetKeys =
        standardFacets.values().stream()
            .map(StandardFacetContract::key)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    verifyAllOfficialFacetSchemasRegistered(schemaDocuments.keySet());
  }

  JsonSchema rootSchema() {
    return rootSchema;
  }

  Optional<StandardFacetContract> standardFacet(AttachmentPoint attachment, String key) {
    return Optional.ofNullable(standardFacets.get(new FacetKey(attachment, key)));
  }

  boolean isStandardFacetKey(String key) {
    return standardFacetKeys.contains(key);
  }

  public Set<String> standardFacetKeys(AttachmentPoint attachment) {
    return standardFacets.values().stream()
        .filter(contract -> contract.attachment() == attachment)
        .map(StandardFacetContract::key)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private JsonSchema schema(String schemaLocation) {
    return schemaFactory.getSchema(SchemaLocation.of(schemaLocation), validatorConfig);
  }

  private Map<String, String> loadSchemas() {
    try (InputStream input = resource(MANIFEST_RESOURCE)) {
      JsonNode manifest = objectMapper.readTree(input);
      if (!"1.45.0".equals(manifest.path("version").asText())) {
        throw new IllegalStateException("Unsupported bundled OpenLineage schema manifest");
      }
      Map<String, String> documents = new LinkedHashMap<>();
      for (JsonNode entry : manifest.path("schemas")) {
        if (!entry.isTextual() || !entry.textValue().startsWith("openlineage/schemas/1.45.0/")) {
          throw new IllegalStateException("Invalid OpenLineage schema resource in manifest");
        }
        String schemaResource = entry.textValue();
        byte[] bytes;
        try (InputStream schemaInput = resource(schemaResource)) {
          bytes = schemaInput.readAllBytes();
        }
        JsonNode schemaNode = objectMapper.readTree(bytes);
        String uri = requiredText(schemaNode, "$id");
        if (!JsonMetaSchema.getV202012().getIri().equals(requiredText(schemaNode, "$schema"))) {
          throw new IllegalStateException(
              "OpenLineage schema identity mismatch for " + schemaResource);
        }
        if (documents.put(uri, new String(bytes, StandardCharsets.UTF_8)) != null) {
          throw new IllegalStateException("Duplicate OpenLineage schema identity " + uri);
        }
      }
      verifyReferenceClosure(documents);
      if (!documents.containsKey(ROOT_SCHEMA_URI)) {
        throw new IllegalStateException("OpenLineage root schema is missing from the manifest");
      }
      return Map.copyOf(documents);
    } catch (IOException exception) {
      throw new IllegalStateException("Unable to load bundled OpenLineage schemas", exception);
    }
  }

  private void verifyReferenceClosure(Map<String, String> documents) throws IOException {
    for (Map.Entry<String, String> document : documents.entrySet()) {
      JsonNode schema = objectMapper.readTree(document.getValue());
      List<String> references = new ArrayList<>();
      collectReferences(schema, references);
      URI base = URI.create(document.getKey());
      for (String reference : references) {
        URI resolved = base.resolve(reference);
        URI documentUri = withoutFragment(resolved);
        if (!documentUri.toString().isEmpty()
            && !documentUri.toString().equals(JsonMetaSchema.getV202012().getIri())
            && !documents.containsKey(documentUri.toString())) {
          throw new IllegalStateException("Incomplete OpenLineage schema closure: " + resolved);
        }
      }
    }
  }

  private static void collectReferences(JsonNode node, List<String> references) {
    if (node.isObject()) {
      node.fields()
          .forEachRemaining(
              entry -> {
                if ("$ref".equals(entry.getKey()) && entry.getValue().isTextual()) {
                  references.add(entry.getValue().textValue());
                } else {
                  collectReferences(entry.getValue(), references);
                }
              });
    } else if (node.isArray()) {
      node.forEach(child -> collectReferences(child, references));
    }
  }

  private static URI withoutFragment(URI uri) {
    String value = uri.toString();
    int fragment = value.indexOf('#');
    return URI.create(fragment >= 0 ? value.substring(0, fragment) : value);
  }

  private static String requiredText(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || !value.isTextual() || value.textValue().isBlank()) {
      throw new IllegalStateException("Invalid OpenLineage schema field " + field);
    }
    return value.textValue();
  }

  private InputStream resource(String name) {
    InputStream input = OpenLineageSchemaCatalog.class.getClassLoader().getResourceAsStream(name);
    if (input == null) {
      throw new IllegalStateException("Missing bundled resource " + name);
    }
    return input;
  }

  private void addRunFacets(Map<FacetKey, StandardFacetContract> facets) {
    add(facets, AttachmentPoint.RUN, "externalQuery", "ExternalQueryRunFacet");
    add(facets, AttachmentPoint.RUN, "gcp_dataproc", "GcpDataprocRunFacet");
    add(facets, AttachmentPoint.RUN, "extractionError", "ExtractionErrorRunFacet");
    add(facets, AttachmentPoint.RUN, "parent", "ParentRunFacet");
    add(facets, AttachmentPoint.RUN, "nominalTime", "NominalTimeRunFacet");
    add(facets, AttachmentPoint.RUN, "tags", "TagsRunFacet");
    add(facets, AttachmentPoint.RUN, "errorMessage", "ErrorMessageRunFacet");
    add(facets, AttachmentPoint.RUN, "environmentVariables", "EnvironmentVariablesRunFacet");
    add(facets, AttachmentPoint.RUN, "gcp_composer_run", "GcpComposerRunFacet");
    add(facets, AttachmentPoint.RUN, "executionParameters", "ExecutionParametersRunFacet");
    add(facets, AttachmentPoint.RUN, "jobDependencies", "JobDependenciesRunFacet");
    add(facets, AttachmentPoint.RUN, "processing_engine", "ProcessingEngineRunFacet");
  }

  private void addJobFacets(Map<FacetKey, StandardFacetContract> facets) {
    add(facets, AttachmentPoint.JOB, "jobType", "JobTypeJobFacet");
    add(facets, AttachmentPoint.JOB, "sourceCode", "SourceCodeJobFacet");
    add(facets, AttachmentPoint.JOB, "gcp_lineage", "GcpLineageJobFacet");
    add(facets, AttachmentPoint.JOB, "sql", "SQLJobFacet");
    add(facets, AttachmentPoint.JOB, "gcp_composer_job", "GcpComposerJobFacet");
    add(facets, AttachmentPoint.JOB, "ownership", "OwnershipJobFacet");
    add(facets, AttachmentPoint.JOB, "sourceCodeLocation", "SourceCodeLocationJobFacet");
    add(facets, AttachmentPoint.JOB, "tags", "TagsJobFacet");
    add(facets, AttachmentPoint.JOB, "documentation", "DocumentationJobFacet");
  }

  private void addDatasetFacets(Map<FacetKey, StandardFacetContract> facets) {
    add(facets, AttachmentPoint.DATASET, "hierarchy", "HierarchyDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "dataSource", "DatasourceDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "version", "DatasetVersionDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "datasetType", "DatasetTypeDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "storage", "StorageDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "columnLineage", "ColumnLineageDatasetFacet");
    add(
        facets,
        AttachmentPoint.DATASET,
        "lifecycleStateChange",
        "LifecycleStateChangeDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "dataQualityMetrics", "DataQualityMetricsDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "tags", "TagsDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "documentation", "DocumentationDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "schema", "SchemaDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "ownership", "OwnershipDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "catalog", "CatalogDatasetFacet");
    add(facets, AttachmentPoint.DATASET, "symlinks", "SymlinksDatasetFacet");
  }

  private void addInputFacets(Map<FacetKey, StandardFacetContract> facets) {
    add(
        facets,
        AttachmentPoint.INPUT_DATASET,
        "dataQualityAssertions",
        "DataQualityAssertionsDatasetFacet");
    add(
        facets,
        AttachmentPoint.INPUT_DATASET,
        "inputStatistics",
        "InputStatisticsInputDatasetFacet");
    add(
        facets,
        AttachmentPoint.INPUT_DATASET,
        "dataQualityMetrics",
        "DataQualityMetricsInputDatasetFacet");
    add(
        facets,
        AttachmentPoint.INPUT_DATASET,
        "subset",
        "BaseSubsetDatasetFacet",
        "InputSubsetInputDatasetFacet");
    add(
        facets,
        AttachmentPoint.INPUT_DATASET,
        "icebergScanReport",
        "IcebergScanReportInputDatasetFacet");
  }

  private void addOutputFacets(Map<FacetKey, StandardFacetContract> facets) {
    add(
        facets,
        AttachmentPoint.OUTPUT_DATASET,
        "outputStatistics",
        "OutputStatisticsOutputDatasetFacet");
    add(
        facets,
        AttachmentPoint.OUTPUT_DATASET,
        "subset",
        "BaseSubsetDatasetFacet",
        "OutputSubsetOutputDatasetFacet");
    add(
        facets,
        AttachmentPoint.OUTPUT_DATASET,
        "icebergCommitReport",
        "IcebergCommitReportOutputDatasetFacet");
  }

  private void add(
      Map<FacetKey, StandardFacetContract> facets,
      AttachmentPoint attachment,
      String key,
      String schemaName) {
    add(facets, attachment, key, schemaName, schemaName);
  }

  private void add(
      Map<FacetKey, StandardFacetContract> facets,
      AttachmentPoint attachment,
      String key,
      String schemaName,
      String definition) {
    URI documentUri = schemaDocumentUri(schemaName);
    StandardFacetContract contract =
        new StandardFacetContract(
            attachment,
            key,
            documentUri,
            definition,
            schema(documentUri + "#/$defs/" + definition));
    if (facets.put(new FacetKey(attachment, key), contract) != null) {
      throw new IllegalStateException(
          "Duplicate standard facet contract " + attachment + ":" + key);
    }
  }

  private URI schemaDocumentUri(String schemaName) {
    URI documentUri = schemaDocumentUrisByName.get(schemaName);
    if (documentUri == null) {
      throw new IllegalStateException("Missing schema " + schemaName);
    }
    return documentUri;
  }

  private static Map<String, URI> indexSchemaDocuments(Set<String> schemaUris) {
    Map<String, URI> documentsByName = new HashMap<>();
    for (String value : schemaUris) {
      URI uri = URI.create(value);
      String path = uri.getPath();
      int slash = path.lastIndexOf('/');
      String fileName = slash >= 0 ? path.substring(slash + 1) : path;
      if (!fileName.endsWith(".json")) {
        throw new IllegalStateException("Invalid OpenLineage schema identity " + value);
      }
      String schemaName = fileName.substring(0, fileName.length() - ".json".length());
      if (documentsByName.put(schemaName, uri) != null) {
        throw new IllegalStateException("Duplicate OpenLineage schema name " + schemaName);
      }
    }
    return Map.copyOf(documentsByName);
  }

  private void verifyAllOfficialFacetSchemasRegistered(Set<String> schemaUris) {
    Set<URI> registered =
        standardFacets.values().stream()
            .map(StandardFacetContract::schemaDocumentUri)
            .collect(java.util.stream.Collectors.toSet());
    for (String uri : schemaUris) {
      if (!ROOT_SCHEMA_URI.equals(uri) && !registered.contains(URI.create(uri))) {
        throw new IllegalStateException(
            "Official OpenLineage facet schema is not registered: " + uri);
      }
    }
  }
}
