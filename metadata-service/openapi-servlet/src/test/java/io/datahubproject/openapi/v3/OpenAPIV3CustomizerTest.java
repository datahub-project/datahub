package io.datahubproject.openapi.v3;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.SpecVersion;
import io.swagger.v3.oas.models.callbacks.Callback;
import io.swagger.v3.oas.models.examples.Example;
import io.swagger.v3.oas.models.headers.Header;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.links.Link;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.security.SecurityScheme;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenAPIV3CustomizerTest {

  private EntityRegistry mockEntityRegistry;
  private ConfigurationProvider mockConfigurationProvider;
  private SpecVersion specVersion;

  @BeforeMethod
  public void setUp() {
    mockEntityRegistry = mock(EntityRegistry.class);
    mockConfigurationProvider = mock(ConfigurationProvider.class);
    specVersion = SpecVersion.V31;
  }

  @Test
  public void testCustomizerMergesComponentsCorrectly() {
    // Setup Spring OpenAPI with existing components
    Map<String, Schema> springSchemas = new HashMap<>();
    springSchemas.put("ExistingSchema", new Schema().type("string").description("Spring schema"));
    springSchemas.put(
        "OverrideSchema", new Schema().type("integer").description("Spring override"));

    Components springComponents = new Components();
    springComponents.setSchemas(springSchemas);

    OpenAPI springOpenAPI =
        new OpenAPI(specVersion)
            .components(springComponents)
            .paths(new Paths())
            .info(new Info().title("Spring API").version("1.0"));

    // Setup Registry OpenAPI that will be returned by generateOpenApiSpec
    Map<String, Schema> registrySchemas = new HashMap<>();
    registrySchemas.put("NewSchema", new Schema().type("boolean").description("Registry schema"));
    registrySchemas.put(
        "OverrideSchema", new Schema().type("object").description("Registry override"));

    Components registryComponents = new Components();
    registryComponents.setSchemas(registrySchemas);

    OpenAPI registryOpenAPI =
        new OpenAPI(specVersion)
            .components(registryComponents)
            .paths(new Paths())
            .info(new Info().title("Registry API").version("2.0"));

    // Mock the static method
    try (MockedStatic<OpenAPIV3Generator> mockedGenerator =
        Mockito.mockStatic(OpenAPIV3Generator.class)) {

      mockedGenerator
          .when(
              () ->
                  OpenAPIV3Generator.generateOpenApiSpec(
                      any(EntityRegistry.class), any(ConfigurationProvider.class)))
          .thenReturn(registryOpenAPI);

      // Execute customizer
      OpenAPIV3Customizer.customizer(springOpenAPI, mockEntityRegistry, mockConfigurationProvider);

      // Verify results
      assertNotNull(springOpenAPI.getComponents());
      Map<String, Schema> mergedSchemas = springOpenAPI.getComponents().getSchemas();
      assertNotNull(mergedSchemas);

      // Should have 3 schemas: ExistingSchema, NewSchema, and OverrideSchema
      assertEquals(3, mergedSchemas.size());

      // ExistingSchema should remain from Spring
      assertTrue(mergedSchemas.containsKey("ExistingSchema"));
      assertEquals("string", mergedSchemas.get("ExistingSchema").getType());
      assertEquals("Spring schema", mergedSchemas.get("ExistingSchema").getDescription());

      // NewSchema should be added from Registry
      assertTrue(mergedSchemas.containsKey("NewSchema"));
      assertEquals("boolean", mergedSchemas.get("NewSchema").getType());
      assertEquals("Registry schema", mergedSchemas.get("NewSchema").getDescription());

      // OverrideSchema should be overridden by Registry
      assertTrue(mergedSchemas.containsKey("OverrideSchema"));
      assertEquals("object", mergedSchemas.get("OverrideSchema").getType());
      assertEquals("Registry override", mergedSchemas.get("OverrideSchema").getDescription());

      // Info should be replaced with Registry info
      assertEquals("Registry API", springOpenAPI.getInfo().getTitle());
      assertEquals("2.0", springOpenAPI.getInfo().getVersion());
    }
  }

  @Test
  public void testCustomizerWithNullComponents() {
    // Spring OpenAPI without components
    OpenAPI springOpenAPI =
        new OpenAPI(specVersion)
            .paths(new Paths())
            .info(new Info().title("Spring API").version("1.0"));

    // Registry OpenAPI with components
    Map<String, Schema> registrySchemas = new HashMap<>();
    registrySchemas.put("RegistrySchema", new Schema().type("string"));

    Components registryComponents = new Components();
    registryComponents.setSchemas(registrySchemas);

    OpenAPI registryOpenAPI =
        new OpenAPI(specVersion)
            .components(registryComponents)
            .paths(new Paths())
            .info(new Info().title("Registry API").version("2.0"));

    try (MockedStatic<OpenAPIV3Generator> mockedGenerator =
        Mockito.mockStatic(OpenAPIV3Generator.class)) {

      mockedGenerator
          .when(
              () ->
                  OpenAPIV3Generator.generateOpenApiSpec(
                      any(EntityRegistry.class), any(ConfigurationProvider.class)))
          .thenReturn(registryOpenAPI);

      // Execute customizer
      OpenAPIV3Customizer.customizer(springOpenAPI, mockEntityRegistry, mockConfigurationProvider);

      // Verify results
      assertNotNull(springOpenAPI.getComponents());
      Map<String, Schema> schemas = springOpenAPI.getComponents().getSchemas();
      assertNotNull(schemas);
      assertEquals(1, schemas.size());
      assertTrue(schemas.containsKey("RegistrySchema"));
    }
  }

  @Test
  public void testCustomizerMergesAllComponentTypes() {
    // Setup comprehensive Spring components
    Components springComponents = new Components();
    springComponents.setSchemas(Map.of("SpringSchema", new Schema().type("string")));
    springComponents.setResponses(
        Map.of("SpringResponse", new ApiResponse().description("Spring")));
    springComponents.setParameters(Map.of("SpringParam", new Parameter().name("spring")));
    springComponents.setExamples(Map.of("SpringExample", new Example().value("spring")));
    springComponents.setRequestBodies(
        Map.of("SpringBody", new RequestBody().description("Spring")));
    springComponents.setHeaders(Map.of("SpringHeader", new Header().description("Spring")));
    springComponents.setSecuritySchemes(
        Map.of("SpringSecurity", new SecurityScheme().type(SecurityScheme.Type.HTTP)));
    springComponents.setLinks(Map.of("SpringLink", new Link().operationId("spring")));
    springComponents.setCallbacks(Map.of("SpringCallback", new Callback()));

    OpenAPI springOpenAPI =
        new OpenAPI(specVersion)
            .components(springComponents)
            .info(new Info().title("Spring API").version("1.0"));

    // Setup comprehensive Registry components with some overlaps
    Components registryComponents = new Components();
    registryComponents.setSchemas(
        Map.of(
            "RegistrySchema", new Schema().type("boolean"),
            "SpringSchema", new Schema().type("object"))); // Override
    registryComponents.setResponses(
        Map.of("RegistryResponse", new ApiResponse().description("Registry")));
    registryComponents.setParameters(Map.of("RegistryParam", new Parameter().name("registry")));

    OpenAPI registryOpenAPI =
        new OpenAPI(specVersion)
            .components(registryComponents)
            .info(new Info().title("Registry API").version("2.0"));

    try (MockedStatic<OpenAPIV3Generator> mockedGenerator =
        Mockito.mockStatic(OpenAPIV3Generator.class)) {

      mockedGenerator
          .when(
              () ->
                  OpenAPIV3Generator.generateOpenApiSpec(
                      any(EntityRegistry.class), any(ConfigurationProvider.class)))
          .thenReturn(registryOpenAPI);

      // Execute customizer
      OpenAPIV3Customizer.customizer(springOpenAPI, mockEntityRegistry, mockConfigurationProvider);

      // Verify all component types are merged correctly
      Components merged = springOpenAPI.getComponents();

      // Schemas
      assertEquals(2, merged.getSchemas().size());
      assertEquals("object", merged.getSchemas().get("SpringSchema").getType()); // Overridden
      assertEquals("boolean", merged.getSchemas().get("RegistrySchema").getType());

      // Responses
      assertEquals(2, merged.getResponses().size());
      assertTrue(merged.getResponses().containsKey("SpringResponse"));
      assertTrue(merged.getResponses().containsKey("RegistryResponse"));

      // Parameters
      assertEquals(2, merged.getParameters().size());
      assertTrue(merged.getParameters().containsKey("SpringParam"));
      assertTrue(merged.getParameters().containsKey("RegistryParam"));

      // Other components should remain from Spring only
      assertEquals(1, merged.getExamples().size());
      assertEquals(1, merged.getRequestBodies().size());
      assertEquals(1, merged.getHeaders().size());
      assertEquals(1, merged.getSecuritySchemes().size());
      assertEquals(1, merged.getLinks().size());
      assertEquals(1, merged.getCallbacks().size());
    }
  }

  @Test
  public void testCustomizerMergesPaths() {
    // Spring OpenAPI with paths
    Paths springPaths = new Paths();
    springPaths.addPathItem("/spring", new PathItem().description("Spring path"));

    OpenAPI springOpenAPI =
        new OpenAPI(specVersion)
            .paths(springPaths)
            .info(new Info().title("Spring API").version("1.0"));

    // Registry OpenAPI with paths
    Paths registryPaths = new Paths();
    registryPaths.addPathItem("/registry", new PathItem().description("Registry path"));
    registryPaths.addPathItem(
        "/spring", new PathItem().description("Registry override")); // Override

    OpenAPI registryOpenAPI =
        new OpenAPI(specVersion)
            .paths(registryPaths)
            .info(new Info().title("Registry API").version("2.0"));

    try (MockedStatic<OpenAPIV3Generator> mockedGenerator =
        Mockito.mockStatic(OpenAPIV3Generator.class)) {

      mockedGenerator
          .when(
              () ->
                  OpenAPIV3Generator.generateOpenApiSpec(
                      any(EntityRegistry.class), any(ConfigurationProvider.class)))
          .thenReturn(registryOpenAPI);

      // Execute customizer
      OpenAPIV3Customizer.customizer(springOpenAPI, mockEntityRegistry, mockConfigurationProvider);

      // Verify paths are merged
      Paths mergedPaths = springOpenAPI.getPaths();
      assertNotNull(mergedPaths);
      assertEquals(2, mergedPaths.size());

      // Registry path should override Spring path
      assertEquals("Registry override", mergedPaths.get("/spring").getDescription());
      assertEquals("Registry path", mergedPaths.get("/registry").getDescription());
    }
  }

  @Test
  public void testMergeWithPrecedenceDirectly() throws Exception {
    // Use reflection to test the private mergeWithPrecedence method
    Method mergeMethod =
        OpenAPIV3Customizer.class.getDeclaredMethod(
            "mergeWithPrecedence", Supplier.class, Supplier.class);
    mergeMethod.setAccessible(true);

    // Test data
    Map<String, String> springMap = new HashMap<>();
    springMap.put("common", "spring-value");
    springMap.put("spring-only", "spring-unique");

    Map<String, String> registryMap = new HashMap<>();
    registryMap.put("common", "registry-value");
    registryMap.put("registry-only", "registry-unique");

    @SuppressWarnings("unchecked")
    Map<String, String> result =
        (Map<String, String>)
            mergeMethod.invoke(
                null,
                (Supplier<Map<String, String>>) () -> springMap,
                (Supplier<Map<String, String>>) () -> registryMap);

    // Verify merge behavior
    assertNotNull(result);
    assertEquals(3, result.size());

    // Registry should override common keys
    assertEquals("registry-value", result.get("common"));

    // Both unique keys should be present
    assertEquals("spring-unique", result.get("spring-only"));
    assertEquals("registry-unique", result.get("registry-only"));
  }

  @Test
  public void testMergeWithPrecedenceNullHandling() throws Exception {
    Method mergeMethod =
        OpenAPIV3Customizer.class.getDeclaredMethod(
            "mergeWithPrecedence", Supplier.class, Supplier.class);
    mergeMethod.setAccessible(true);

    // Test with null spring map
    Map<String, String> registryMap = Map.of("key", "value");

    @SuppressWarnings("unchecked")
    Map<String, String> result1 =
        (Map<String, String>)
            mergeMethod.invoke(
                null,
                (Supplier<Map<String, String>>) () -> null,
                (Supplier<Map<String, String>>) () -> registryMap);

    assertEquals(1, result1.size());
    assertEquals("value", result1.get("key"));

    // Test with null registry map
    Map<String, String> springMap = Map.of("key", "value");

    @SuppressWarnings("unchecked")
    Map<String, String> result2 =
        (Map<String, String>)
            mergeMethod.invoke(
                null,
                (Supplier<Map<String, String>>) () -> springMap,
                (Supplier<Map<String, String>>) () -> null);

    assertEquals(1, result2.size());
    assertEquals("value", result2.get("key"));

    // Test with both null
    @SuppressWarnings("unchecked")
    Map<String, String> result3 =
        (Map<String, String>)
            mergeMethod.invoke(
                null,
                (Supplier<Map<String, String>>) () -> null,
                (Supplier<Map<String, String>>) () -> null);

    assertNotNull(result3);
    assertTrue(result3.isEmpty());
  }
}
