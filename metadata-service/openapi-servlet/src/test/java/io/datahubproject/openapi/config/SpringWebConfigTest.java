package io.datahubproject.openapi.config;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.v3.OpenAPIV3Generator;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.SpecVersion;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Schema;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springdoc.core.providers.ObjectMapperProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(classes = {ConfigurationProvider.class, SpringWebConfig.class})
@Import({SpringWebConfigTestConfiguration.class})
@TestPropertySource(locations = "classpath:/application.yaml")
public class SpringWebConfigTest extends AbstractTestNGSpringContextTests {

  OperationContext operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
  @Autowired private ConfigurationProvider configurationProvider;
  @Autowired private ObjectMapperProvider objectMapperProvider;
  @Autowired private List<OpenAPI> openAPIs;

  @Test
  void testComponentsMergeWithDuplicateKeys() {
    // Setup
    SpringWebConfig config = new SpringWebConfig();

    // Create test schemas with duplicate keys
    Map<String, Schema> schemas1 = new HashMap<>();
    schemas1.put("TestSchema", new Schema().type("string").description("First schema"));

    Map<String, Schema> schemas2 = new HashMap<>();
    schemas2.put("TestSchema", new Schema().type("object").description("Second schema"));

    SpecVersion specVersion = SpecVersion.V31; // Use the same version as in OpenAPIV3Generator

    OpenAPI openApi1 =
        new OpenAPI(specVersion)
            .components(new Components().schemas(schemas1))
            .paths(new Paths())
            .info(new Info().title("Test API").version("1.0"));

    OpenAPI openApi2 =
        new OpenAPI(specVersion)
            .components(new Components().schemas(schemas2))
            .paths(new Paths())
            .info(new Info().title("Test API").version("2.0"));

    // Mock OpenAPIV3Generator
    try (MockedStatic<OpenAPIV3Generator> mockedGenerator =
        Mockito.mockStatic(OpenAPIV3Generator.class)) {

      // Set up the mock BEFORE calling the method under test
      mockedGenerator
          .when(
              () ->
                  OpenAPIV3Generator.generateOpenApiSpec(
                      Mockito.any(EntityRegistry.class), Mockito.any(ConfigurationProvider.class)))
          .thenReturn(openApi2);

      // Get the GroupedOpenApi - this will call generateOpenApiSpec internally
      var groupedApi =
          config.v3OpenApiGroup(operationContext.getEntityRegistry(), configurationProvider);

      // The customizer is what actually performs the merge
      // We need to simulate what Spring does when it builds the OpenAPI spec
      assertEquals(1, groupedApi.getOpenApiCustomizers().size());

      // Apply the customizer to openApi1 (simulating Spring's behavior)
      groupedApi.getOpenApiCustomizers().forEach(customizer -> customizer.customise(openApi1));

      // Verify the merged components
      Map<String, Schema> mergedSchemas = openApi1.getComponents().getSchemas();

      // Assert that we have the expected number of schemas
      assertEquals(1, mergedSchemas.size());

      // Assert that the duplicate key contains the second schema (registry/v3 value takes
      // precedence)
      Schema resultSchema = mergedSchemas.get("TestSchema");
      assertNotNull(resultSchema);
      assertEquals("object", resultSchema.getType());
      assertEquals("Second schema", resultSchema.getDescription());

      // Verify that info was also merged from the registry OpenAPI
      assertNotNull(openApi1.getInfo());
      assertEquals("Test API", openApi1.getInfo().getTitle());
      assertEquals("2.0", openApi1.getInfo().getVersion());
    }
  }

  @Test
  void testV31EnumSerialization() throws JsonProcessingException {
    assertNotNull(openAPIs);
    List<OpenAPI> withSchema =
        openAPIs.stream()
            .filter(o -> o.getComponents().getSchemas().containsKey("VersioningScheme"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(withSchema.size(), 1);
    OpenAPI openAPI = withSchema.get(0);
    assertEquals(openAPI.getOpenapi(), "3.1.0");

    Schema schema = openAPI.getComponents().getSchemas().get("VersioningScheme");
    assertEquals("string", schema.getType());

    // Use SpringDoc's mapper, not Json.mapper()
    String json =
        objectMapperProvider
            .jsonMapper()
            .writerWithDefaultPrettyPrinter()
            .writeValueAsString(openAPI);

    assertTrue(json.contains("\"VersioningScheme\" : {\n        \"type\" : \"string\""));
    assertTrue(json.contains("\"openapi\" : \"3.1.0\""));
  }
}
