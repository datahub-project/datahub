package io.datahubproject.openapi.openlineage.validation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonMetaSchema;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog.AttachmentPoint;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class OpenLineageSchemaCatalogTest {
  private static final String RESOURCE_ROOT = "openlineage/schemas/1.45.0/";
  private static final String MANIFEST_RESOURCE = RESOURCE_ROOT + "manifest.json";
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testBundledSchemaCatalogIsCompleteAndClosed() throws Exception {
    JsonNode manifest = readResource(MANIFEST_RESOURCE);
    assertEquals(manifest.path("version").textValue(), "1.45.0");

    Map<URI, JsonNode> schemas = new HashMap<>();
    Set<String> schemaNames = new HashSet<>();
    for (JsonNode entry : manifest.path("schemas")) {
      assertTrue(entry.isTextual());
      assertTrue(entry.textValue().startsWith(RESOURCE_ROOT));
      JsonNode schema = readResource(entry.textValue());
      assertEquals(schema.path("$schema").textValue(), JsonMetaSchema.getV202012().getIri());
      URI id = URI.create(requiredText(schema, "$id"));
      assertTrue(schemas.put(id, schema) == null, "Duplicate schema identity " + id);
      String fileName = id.getPath().substring(id.getPath().lastIndexOf('/') + 1);
      assertTrue(fileName.endsWith(".json"));
      assertTrue(
          schemaNames.add(fileName.substring(0, fileName.length() - ".json".length())),
          "Duplicate schema name " + fileName);
    }

    assertTrue(schemas.containsKey(URI.create(OpenLineageSchemaCatalog.ROOT_SCHEMA_URI)));
    assertReferenceClosure(schemas);

    OpenLineageSchemaCatalog catalog = new OpenLineageSchemaCatalog();
    Set<URI> registered = new HashSet<>();
    for (AttachmentPoint attachment : AttachmentPoint.values()) {
      for (String key : catalog.standardFacetKeys(attachment)) {
        OpenLineageSchemaCatalog.StandardFacetContract contract =
            catalog.standardFacet(attachment, key).orElseThrow();
        assertNotNull(contract.schema());
        registered.add(contract.schemaDocumentUri());
      }
    }
    Set<URI> facetSchemas = new HashSet<>(schemas.keySet());
    facetSchemas.remove(URI.create(OpenLineageSchemaCatalog.ROOT_SCHEMA_URI));
    assertEquals(registered, facetSchemas);
  }

  private void assertReferenceClosure(Map<URI, JsonNode> schemas) {
    for (Map.Entry<URI, JsonNode> document : schemas.entrySet()) {
      List<String> references = new ArrayList<>();
      collectReferences(document.getValue(), references);
      for (String reference : references) {
        URI resolved = document.getKey().resolve(reference);
        URI documentUri = withoutFragment(resolved);
        assertTrue(
            documentUri.toString().isEmpty()
                || documentUri.toString().equals(JsonMetaSchema.getV202012().getIri())
                || schemas.containsKey(documentUri),
            "Missing schema reference " + resolved);
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

  private JsonNode readResource(String name) throws Exception {
    try (InputStream input = getClass().getClassLoader().getResourceAsStream(name)) {
      assertNotNull(input, "Missing bundled resource " + name);
      return objectMapper.readTree(input);
    }
  }

  private static String requiredText(JsonNode node, String field) {
    JsonNode value = node.get(field);
    assertNotNull(value, "Missing schema field " + field);
    assertTrue(value.isTextual() && !value.textValue().isBlank());
    return value.textValue();
  }

  private static URI withoutFragment(URI uri) {
    String value = uri.toString();
    int fragment = value.indexOf('#');
    return URI.create(fragment >= 0 ? value.substring(0, fragment) : value);
  }
}
