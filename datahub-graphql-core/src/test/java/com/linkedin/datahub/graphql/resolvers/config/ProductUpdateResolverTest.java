package com.linkedin.datahub.graphql.resolvers.config;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.ProductUpdate;
import com.linkedin.metadata.service.ProductUpdateService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProductUpdateResolverTest {

  @Mock private ProductUpdateService mockProductUpdateService;
  @Mock private FeatureFlags mockFeatureFlags;
  @Mock private DataFetchingEnvironment mockDataFetchingEnvironment;

  private ProductUpdateResolver resolver;
  private ObjectMapper objectMapper;

  @BeforeMethod
  public void setupTest() {
    MockitoAnnotations.openMocks(this);
    objectMapper = new ObjectMapper();
    resolver = new ProductUpdateResolver(mockProductUpdateService, mockFeatureFlags);
  }

  @Test
  public void testGetProductUpdateSuccess() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"description\": \"New features\","
            + "\"image\": \"https://example.com/image.png\","
            + "\"ctaText\": \"Learn more\","
            + "\"ctaLink\": \"https://example.com\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService).getLatestProductUpdate();
    verify(mockProductUpdateService, never()).clearCache(); // Should NOT clear cache
    assertNotNull(result);
    assertTrue(result.getEnabled());
    assertEquals(result.getId(), "v1.0.0");
    assertEquals(result.getTitle(), "What's New");
    assertEquals(result.getDescription(), "New features");
    assertEquals(result.getImage(), "https://example.com/image.png");
    assertEquals(result.getCtaText(), "Learn more");
    assertEquals(result.getCtaLink(), "https://example.com");
  }

  @Test
  public void testGetProductUpdateMinimalFields() throws Exception {
    // Setup - only required fields
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{" + "\"enabled\": true," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    assertNotNull(result);
    assertTrue(result.getEnabled());
    assertEquals(result.getId(), "v1.0.0");
    assertEquals(result.getTitle(), "What's New");
    assertNull(result.getDescription());
    assertNull(result.getImage());
    assertEquals(result.getCtaText(), "Learn more"); // default value
    assertEquals(result.getCtaLink(), ""); // default value
  }

  @Test
  public void testGetProductUpdateFeatureDisabled() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(false);

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService, never()).getLatestProductUpdate();
    assertNull(result);
  }

  @Test
  public void testGetProductUpdateNoJsonAvailable() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.empty());

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService).getLatestProductUpdate();
    assertNull(result);
  }

  @Test
  public void testGetProductUpdateMissingEnabledField() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString = "{" + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    assertNull(result);
  }

  @Test
  public void testGetProductUpdateMissingIdField() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString = "{" + "\"enabled\": true," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    assertNull(result);
  }

  @Test
  public void testGetProductUpdateMissingTitleField() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString = "{" + "\"enabled\": true," + "\"id\": \"v1.0.0\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    assertNull(result);
  }

  @Test
  public void testGetProductUpdateDisabledInJson() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{" + "\"enabled\": false," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    assertNull(result);
  }

  @Test
  public void testGetProductUpdateExceptionHandling() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);
    when(mockProductUpdateService.getLatestProductUpdate())
        .thenThrow(new RuntimeException("Service error"));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify - should handle exception gracefully and return null
    assertNull(result);
  }

  @Test
  public void testGetProductUpdateWithCustomCta() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v2.0.0\","
            + "\"title\": \"Major Update\","
            + "\"ctaText\": \"View Release Notes\","
            + "\"ctaLink\": \"https://docs.example.com/v2.0\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    assertNotNull(result);
    assertEquals(result.getCtaText(), "View Release Notes");
    assertEquals(result.getCtaLink(), "https://docs.example.com/v2.0");
  }

  @Test
  public void testGetProductUpdateNullValues() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"description\": null,"
            + "\"image\": null"
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify - Jackson's asText() converts null JSON values to string "null"
    assertNotNull(result);
    assertEquals(result.getDescription(), "null");
    assertEquals(result.getImage(), "null");
  }

  @Test
  public void testGetProductUpdateUsesCache() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{" + "\"enabled\": true," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute multiple times
    resolver.get(mockDataFetchingEnvironment).get();
    resolver.get(mockDataFetchingEnvironment).get();
    resolver.get(mockDataFetchingEnvironment).get();

    // Verify - service is called each time (caching happens in the service layer)
    verify(mockProductUpdateService, times(3)).getLatestProductUpdate();
    // But clearCache should never be called by this resolver
    verify(mockProductUpdateService, never()).clearCache();
  }

  @Test
  public void testGetProductUpdateEmptyStrings() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"\","
            + "\"title\": \"\","
            + "\"description\": \"\","
            + "\"ctaText\": \"\","
            + "\"ctaLink\": \"\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify - empty strings should be accepted
    assertNotNull(result);
    assertEquals(result.getId(), "");
    assertEquals(result.getTitle(), "");
    assertEquals(result.getDescription(), "");
    assertEquals(result.getCtaText(), "");
    assertEquals(result.getCtaLink(), "");
  }
}
