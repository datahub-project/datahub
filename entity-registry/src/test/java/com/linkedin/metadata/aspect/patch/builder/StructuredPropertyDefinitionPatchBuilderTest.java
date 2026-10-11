package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.structuredproperty.StructuredPropertyDefinitionTemplate;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredPropertyDefinition;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
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
  public void testSetAllowedValuesReplacesArrayInOrder() {
    PropertyValue first =
        new PropertyValue().setValue(PrimitivePropertyValue.create("Gold")).setDescription("first");
    PropertyValue second =
        new PropertyValue()
            .setValue(PrimitivePropertyValue.create("Silver"))
            .setDescription("second");

    builder.setAllowedValues(List.of(first, second));

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> op = pathValues.get(0);
    assertEquals(op.getLeft(), "replace");
    assertEquals(op.getMiddle(), "/allowedValues");
    assertTrue(op.getRight().isArray());
    assertEquals(op.getRight().size(), 2);
    assertEquals(op.getRight().get(0).get("value").get("string").asText(), "Gold");
    assertEquals(op.getRight().get(1).get("value").get("string").asText(), "Silver");
  }

  @Test
  public void testSetAllowedValuesAppliesInOrder() throws Exception {
    PropertyValue bronze = new PropertyValue().setValue(PrimitivePropertyValue.create("Bronze"));
    PropertyValue gold = new PropertyValue().setValue(PrimitivePropertyValue.create("Gold"));
    StructuredPropertyDefinition existing =
        new StructuredPropertyDefinition()
            .setQualifiedName("io.acryl.classification")
            .setDisplayName("Classification")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setCardinality(PropertyCardinality.SINGLE)
            .setEntityTypes(new UrnArray())
            .setAllowedValues(new PropertyValueArray(bronze, gold));

    MetadataChangeProposal proposal = builder.setAllowedValues(List.of(gold, bronze)).build();
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        proposal.getAspect().getValue().asString(StandardCharsets.UTF_8)))
                .readArray());
    StructuredPropertyDefinition result =
        new StructuredPropertyDefinitionTemplate().applyPatch(existing, patch);

    assertEquals(result.getAllowedValues().get(0).getValue(), gold.getValue());
    assertEquals(result.getAllowedValues().get(1).getValue(), bronze.getValue());
  }
}
