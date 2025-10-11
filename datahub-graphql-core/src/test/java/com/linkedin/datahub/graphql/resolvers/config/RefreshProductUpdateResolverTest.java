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

public class RefreshProductUpdateResolverTest {

  @Mock private ProductUpdateService mockProductUpdateService;
  @Mock private FeatureFlags mockFeatureFlags;
  @Mock private DataFetchingEnvironment mockDataFetchingEnvironment;

  private RefreshProductUpdateResolver resolver;
  private ObjectMapper objectMapper;

  @BeforeMethod
  public void setupTest() {
    MockitoAnnotations.openMocks(this);
    objectMapper = new ObjectMapper();
    resolver = new RefreshProductUpdateResolver(mockProductUpdateService, mockFeatureFlags);
  }

  @Test
  public void testRefreshProductUpdateSuccess() throws Exception {
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
    verify(mockProductUpdateService).clearCache();
    verify(mockProductUpdateService).getLatestProductUpdate();
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
  public void testRefreshProductUpdateMinimalFields() throws Exception {
    // Setup - only required fields
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{" + "\"enabled\": true," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService).clearCache();
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
  public void testRefreshProductUpdateWithCustomCta() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaText\": \"View Release Notes\","
            + "\"ctaLink\": \"https://docs.example.com/releases\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    assertNotNull(result);
    assertEquals(result.getCtaText(), "View Release Notes");
    assertEquals(result.getCtaLink(), "https://docs.example.com/releases");
  }

  @Test
  public void testRefreshProductUpdateFeatureDisabled() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(false);

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService, never()).clearCache();
    verify(mockProductUpdateService, never()).getLatestProductUpdate();
    assertNull(result);
  }

  @Test
  public void testRefreshProductUpdateNoJsonAvailable() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.empty());

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService).clearCache();
    verify(mockProductUpdateService).getLatestProductUpdate();
    assertNull(result);
  }

  @Test
  public void testRefreshProductUpdateMissingEnabledField() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString = "{" + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService).clearCache();
    assertNull(result);
  }

  @Test
  public void testRefreshProductUpdateMissingIdField() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString = "{" + "\"enabled\": true," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService).clearCache();
    assertNull(result);
  }

  @Test
  public void testRefreshProductUpdateMissingTitleField() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString = "{" + "\"enabled\": true," + "\"id\": \"v1.0.0\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService).clearCache();
    assertNull(result);
  }

  @Test
  public void testRefreshProductUpdateDisabledInJson() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{" + "\"enabled\": false," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify
    verify(mockProductUpdateService).clearCache();
    assertNull(result);
  }

  @Test
  public void testRefreshProductUpdateExceptionHandling() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);
    when(mockProductUpdateService.getLatestProductUpdate())
        .thenThrow(new RuntimeException("Service error"));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify - should handle exception gracefully and return null
    verify(mockProductUpdateService).clearCache();
    assertNull(result);
  }

  @Test
  public void testRefreshProductUpdateClearCacheException() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);
    doThrow(new RuntimeException("Cache clear error")).when(mockProductUpdateService).clearCache();

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify - should handle exception gracefully and return null
    assertNull(result);
  }

  @Test
  public void testRefreshProductUpdateInvalidJsonStructure() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    // Create a JSON node with wrong types
    String jsonString =
        "{" + "\"enabled\": \"not-a-boolean\"," + "\"id\": 12345," + "\"title\": true" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    ProductUpdate result = resolver.get(mockDataFetchingEnvironment).get();

    // Verify - enabled: "not-a-boolean" will be parsed as false by Jackson's asBoolean()
    // Since enabled is false, the resolver will return null
    verify(mockProductUpdateService).clearCache();
    assertNull(result);
  }

  @Test
  public void testRefreshProductUpdateVerifiesCacheClearCalled() throws Exception {
    // Setup
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(true);

    String jsonString =
        "{" + "\"enabled\": true," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    when(mockProductUpdateService.getLatestProductUpdate()).thenReturn(Optional.of(jsonNode));

    // Execute
    resolver.get(mockDataFetchingEnvironment).get();

    // Verify - clearCache should always be called before fetching
    verify(mockProductUpdateService, times(1)).clearCache();
    verify(mockProductUpdateService, times(1)).getLatestProductUpdate();
  }

  @Test
  public void testRefreshProductUpdateNullValues() throws Exception {
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
}
