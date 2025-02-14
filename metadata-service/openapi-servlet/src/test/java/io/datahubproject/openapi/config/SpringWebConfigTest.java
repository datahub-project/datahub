package io.datahubproject.openapi.config;

import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import io.datahubproject.openapi.v3.OpenAPIV3Generator;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import java.util.HashMap;
import java.util.Map;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@Import(ConfigurationProvider.class)
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
public class SpringWebConfigTest extends AbstractTestNGSpringContextTests {
  @Autowired private ConfigurationProvider configurationProvider;

  @Test
  void testComponentsMergeWithDuplicateKeys() {
    // Setup
    SpringWebConfig config = new SpringWebConfig();
    EntityRegistry entityRegistry = mock(EntityRegistry.class);

    // Create test schemas with duplicate keys
    Map<String, Schema> schemas1 = new HashMap<>();
    schemas1.put("TestSchema", new Schema().type("string").description("First schema"));

    Map<String, Schema> schemas2 = new HashMap<>();
    schemas2.put("TestSchema", new Schema().type("object").description("Second schema"));

    // Create OpenAPI objects with proper initialization
    OpenAPI openApi1 =
        new OpenAPI().components(new Components().schemas(schemas1)).paths(new Paths());

    OpenAPI openApi2 =
        new OpenAPI().components(new Components().schemas(schemas2)).paths(new Paths());

    // Mock OpenAPIV3Generator
    try (MockedStatic<OpenAPIV3Generator> mockedGenerator =
        Mockito.mockStatic(OpenAPIV3Generator.class)) {
      mockedGenerator
          .when(
              () ->
                  OpenAPIV3Generator.generateOpenApiSpec(
                      Mockito.any(EntityRegistry.class), Mockito.any(ConfigurationProvider.class)))
          .thenReturn(openApi2);

      // Get the GroupedOpenApi
      var groupedApi = config.v3OpenApiGroup(entityRegistry, configurationProvider);

      // Execute the customizer
      groupedApi.getOpenApiCustomizers().get(0).customise(openApi1);

      // Verify the merged components
      Map<String, Schema> mergedSchemas = openApi1.getComponents().getSchemas();

      // Assert that we have the expected number of schemas
      assertEquals(1, mergedSchemas.size());

      // Assert that the duplicate key contains the second schema (v2 value)
      Schema resultSchema = mergedSchemas.get("TestSchema");
      assertEquals("object", resultSchema.getType());
      assertEquals("Second schema", resultSchema.getDescription());
    }
  }
}
