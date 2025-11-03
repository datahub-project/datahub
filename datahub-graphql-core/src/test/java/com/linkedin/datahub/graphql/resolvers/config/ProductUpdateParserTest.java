package com.linkedin.datahub.graphql.resolvers.config;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.graphql.generated.ProductUpdate;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SuppressWarnings("null")
public class ProductUpdateParserTest {

  private ObjectMapper objectMapper;

  @BeforeMethod
  public void setupTest() {
    objectMapper = new ObjectMapper();
  }

  @Test
  public void testParseProductUpdateSuccess() throws Exception {
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

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

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
  public void testParseProductUpdateMinimalFields() throws Exception {
    String jsonString =
        "{" + "\"enabled\": true," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertTrue(result.getEnabled());
    assertEquals(result.getId(), "v1.0.0");
    assertEquals(result.getTitle(), "What's New");
    assertNull(result.getDescription());
    assertNull(result.getImage());
    assertEquals(result.getCtaText(), "Learn more");
    assertEquals(result.getCtaLink(), "");
  }

  @Test
  public void testParseProductUpdateWithCustomCta() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v2.0.0\","
            + "\"title\": \"Major Update\","
            + "\"ctaText\": \"View Release Notes\","
            + "\"ctaLink\": \"https://docs.example.com/v2.0\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertEquals(result.getCtaText(), "View Release Notes");
    assertEquals(result.getCtaLink(), "https://docs.example.com/v2.0");
  }

  @Test
  public void testParseProductUpdateEmptyOptional() {
    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.empty());

    assertNull(result);
  }

  @Test
  public void testParseProductUpdateMissingEnabledField() throws Exception {
    String jsonString = "{" + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNull(result);
  }

  @Test
  public void testParseProductUpdateMissingIdField() throws Exception {
    String jsonString = "{" + "\"enabled\": true," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNull(result);
  }

  @Test
  public void testParseProductUpdateMissingTitleField() throws Exception {
    String jsonString = "{" + "\"enabled\": true," + "\"id\": \"v1.0.0\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNull(result);
  }

  @Test
  public void testParseProductUpdateDisabledInJson() throws Exception {
    String jsonString =
        "{" + "\"enabled\": false," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNull(result);
  }

  @Test
  public void testParseProductUpdateNullValues() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"description\": null,"
            + "\"image\": null"
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getDescription());
    assertNotNull(result.getImage());
    assertEquals(result.getDescription(), "null");
    assertEquals(result.getImage(), "null");
  }

  @Test
  public void testParseProductUpdateEmptyStrings() throws Exception {
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

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getId());
    assertNotNull(result.getTitle());
    assertNotNull(result.getDescription());
    assertNotNull(result.getCtaText());
    assertNotNull(result.getCtaLink());
    assertEquals(result.getId(), "");
    assertEquals(result.getTitle(), "");
    assertEquals(result.getDescription(), "");
    assertEquals(result.getCtaText(), "");
    assertEquals(result.getCtaLink(), "");
  }

  @Test
  public void testParseProductUpdateEnabledFalse() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": false,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"description\": \"This should not be shown\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNull(result);
  }

  @Test
  public void testParseProductUpdatePartialOptionalFields() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"description\": \"Has description but no image\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getDescription());
    assertEquals(result.getDescription(), "Has description but no image");
    assertNull(result.getImage());
  }

  @Test
  public void testParseProductUpdateImageOnly() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"image\": \"https://example.com/image.png\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNull(result.getDescription());
    assertNotNull(result.getImage());
    assertEquals(result.getImage(), "https://example.com/image.png");
  }

  @Test
  public void testParseProductUpdateBooleanTypesForEnabled() throws Exception {
    String jsonString =
        "{" + "\"enabled\": 1," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getEnabled());
    assertTrue(result.getEnabled());
  }

  @Test
  public void testParseProductUpdateZeroEnabledField() throws Exception {
    String jsonString =
        "{" + "\"enabled\": 0," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNull(result);
  }

  @Test
  public void testParseProductUpdateExtraFields() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"unknownField\": \"Should be ignored\","
            + "\"anotherField\": 12345"
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getId());
    assertNotNull(result.getTitle());
    assertEquals(result.getId(), "v1.0.0");
    assertEquals(result.getTitle(), "What's New");
  }

  @Test
  public void testParseProductUpdateCtaTextWithoutLink() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaText\": \"Click here\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getCtaText());
    assertNotNull(result.getCtaLink());
    assertEquals(result.getCtaText(), "Click here");
    assertEquals(result.getCtaLink(), "");
  }

  @Test
  public void testParseProductUpdateCtaLinkWithoutText() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getCtaText());
    assertNotNull(result.getCtaLink());
    assertEquals(result.getCtaText(), "Learn more");
    assertEquals(result.getCtaLink(), "https://example.com");
  }

  @Test
  public void testParseProductUpdateSpecialCharactersInFields() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0-rc.1+build.123\","
            + "\"title\": \"What's New: <Special> & \\\"Quoted\\\"\","
            + "\"description\": \"Line 1\\nLine 2\\tTabbed\","
            + "\"ctaLink\": \"https://example.com/path?query=value&other=123#anchor\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getId());
    assertNotNull(result.getTitle());
    assertNotNull(result.getDescription());
    assertNotNull(result.getCtaLink());
    assertEquals(result.getId(), "v1.0.0-rc.1+build.123");
    assertEquals(result.getTitle(), "What's New: <Special> & \"Quoted\"");
    assertEquals(result.getDescription(), "Line 1\nLine 2\tTabbed");
    assertEquals(result.getCtaLink(), "https://example.com/path?query=value&other=123#anchor");
  }

  @Test
  public void testParseProductUpdateUnicodeCharacters() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"新機能 🎉\","
            + "\"description\": \"Nouveau fonctionnalités 中文测试\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getTitle());
    assertNotNull(result.getDescription());
    assertEquals(result.getTitle(), "新機能 🎉");
    assertEquals(result.getDescription(), "Nouveau fonctionnalités 中文测试");
  }

  @Test
  public void testParseProductUpdateLongStrings() throws Exception {
    String longDescription = "x".repeat(10000);
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"description\": \""
            + longDescription
            + "\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertNotNull(result.getDescription());
    assertEquals(result.getDescription().length(), 10000);
  }

  @Test
  public void testParseProductUpdateWithClientId() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    String clientId = "abc-123-def-456";

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), clientId);

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "https://example.com?q=abc-123-def-456");
  }

  @Test
  public void testParseProductUpdateWithClientIdAndExistingQueryParams() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com?foo=bar\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    String clientId = "abc-123-def-456";

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), clientId);

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "https://example.com?foo=bar&q=abc-123-def-456");
  }

  @Test
  public void testParseProductUpdateWithClientIdMultipleQueryParams() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com?foo=bar&baz=qux#anchor\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    String clientId = "test-uuid";

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), clientId);

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "https://example.com?foo=bar&baz=qux#anchor&q=test-uuid");
  }

  @Test
  public void testParseProductUpdateWithNullClientId() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), null);

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "https://example.com");
  }

  @Test
  public void testParseProductUpdateWithEmptyClientId() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), "");

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "https://example.com");
  }

  @Test
  public void testParseProductUpdateWithWhitespaceClientId() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), "   ");

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "https://example.com");
  }

  @Test
  public void testParseProductUpdateWithClientIdAndEmptyCtaLink() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    String clientId = "abc-123";

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), clientId);

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "");
  }

  @Test
  public void testParseProductUpdateWithClientIdAndNoCtaLink() throws Exception {
    String jsonString =
        "{" + "\"enabled\": true," + "\"id\": \"v1.0.0\"," + "\"title\": \"What's New\"" + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    String clientId = "abc-123";

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), clientId);

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "");
  }

  @Test
  public void testParseProductUpdateWithClientIdSpecialCharacters() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    String clientId = "abc 123+def/456";

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), clientId);

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "https://example.com?q=abc+123%2Bdef%2F456");
  }

  @Test
  public void testParseProductUpdateWithClientIdUnicodeCharacters() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    String clientId = "测试-client-id-🎉";

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode), clientId);

    assertNotNull(result);
    assertTrue(result.getCtaLink().startsWith("https://example.com?q="));
    assertTrue(result.getCtaLink().contains("%"));
  }

  @Test
  public void testParseProductUpdateBackwardCompatibilityWithoutClientId() throws Exception {
    String jsonString =
        "{"
            + "\"enabled\": true,"
            + "\"id\": \"v1.0.0\","
            + "\"title\": \"What's New\","
            + "\"ctaLink\": \"https://example.com\""
            + "}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    ProductUpdate result = ProductUpdateParser.parseProductUpdate(Optional.of(jsonNode));

    assertNotNull(result);
    assertEquals(result.getCtaLink(), "https://example.com");
  }
}
