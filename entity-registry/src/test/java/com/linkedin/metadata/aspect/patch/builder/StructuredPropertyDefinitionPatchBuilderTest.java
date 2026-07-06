package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyValue;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StructuredPropertyDefinitionPatchBuilderTest {

  private static final String TEST_PROPERTY_URN =
      "urn:li:structuredProperty:io.acryl.classification";

  private TestableBuilder builder;

  // Expose protected pathValues for assertions.
  private static class TestableBuilder extends StructuredPropertyDefinitionPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableBuilder();
    builder.urn(Urn.createFromString(TEST_PROPERTY_URN));
  }

  @Test
  public void testAddAllowedPlatform() {
    String platformUrn = "urn:li:dataPlatform:bigquery";
    builder.addAllowedPlatform(platformUrn);

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> op = pathValues.get(0);
    assertEquals(op.getLeft(), "add");
    assertTrue(
        op.getMiddle().startsWith("/allowedPlatforms/"), "Path should be under /allowedPlatforms/");
    assertTrue(op.getMiddle().endsWith(platformUrn), "Path should end with the platform URN");
    assertEquals(op.getRight().asText(), platformUrn, "Value should be the platform URN");
  }

  @Test
  public void testAddMultipleAllowedPlatforms() {
    builder
        .addAllowedPlatform("urn:li:dataPlatform:bigquery")
        .addAllowedPlatform("urn:li:dataPlatform:snowflake");

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertEquals(pathValues.size(), 2);

    List<String> paths = pathValues.stream().map(ImmutableTriple::getMiddle).toList();
    assertTrue(paths.stream().anyMatch(p -> p.contains("bigquery")));
    assertTrue(paths.stream().anyMatch(p -> p.contains("snowflake")));
  }

  @Test
  public void testAddAllowedValueEscapesSlash() {
    PrimitivePropertyValue primitive = new PrimitivePropertyValue();
    primitive.setString("SITS/eVision");
    PropertyValue propertyValue = new PropertyValue();
    propertyValue.setValue(primitive);
    builder.addAllowedValue(propertyValue);

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> op = pathValues.get(0);
    assertEquals(op.getLeft(), "add");

    String prefix = "/allowedValues/";
    assertTrue(op.getMiddle().startsWith(prefix), "Path should be under /allowedValues/");
    String segment = op.getMiddle().substring(prefix.length());
    // The allowed value must be a single JSON Pointer segment - no raw '/' may leak
    // through, or it would be tokenised into extra path segments on apply.
    assertFalse(segment.contains("/"), "Slash in the allowed value must be escaped, not left raw");
    assertTrue(segment.contains("~1"), "Slash should be encoded as ~1 per RFC 6901");
    // Decoding (~1 -> /, ~0 -> ~) round-trips back to the original value.
    String decoded = segment.replace("~1", "/").replace("~0", "~");
    assertEquals(decoded, String.valueOf(propertyValue.getValue()));
  }

  @Test
  public void testAddAllowedValueEscapesTilde() {
    PrimitivePropertyValue primitive = new PrimitivePropertyValue();
    primitive.setString("grade~level");
    PropertyValue propertyValue = new PropertyValue();
    propertyValue.setValue(primitive);
    builder.addAllowedValue(propertyValue);

    ImmutableTriple<String, String, JsonNode> op = builder.getTestPathValues().get(0);
    String prefix = "/allowedValues/";
    assertTrue(op.getMiddle().startsWith(prefix));
    String segment = op.getMiddle().substring(prefix.length());
    assertTrue(segment.contains("~0"), "Tilde should be encoded as ~0 per RFC 6901");
    String decoded = segment.replace("~1", "/").replace("~0", "~");
    assertEquals(decoded, String.valueOf(propertyValue.getValue()));
  }
}
