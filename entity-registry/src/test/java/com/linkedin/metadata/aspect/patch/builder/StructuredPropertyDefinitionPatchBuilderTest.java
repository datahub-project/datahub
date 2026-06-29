package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
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
        op.getMiddle().startsWith("/allowedPlatforms/"),
        "Path should be under /allowedPlatforms/");
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
}
