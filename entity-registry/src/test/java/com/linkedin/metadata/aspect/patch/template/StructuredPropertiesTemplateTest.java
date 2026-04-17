package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.common.StructuredPropertiesTemplate;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StructuredPropertiesTemplateTest {

  private static final StructuredPropertiesTemplate TEMPLATE = new StructuredPropertiesTemplate();

  /** Builds an attributed StructuredPropertyValueAssignment. */
  private static StructuredPropertyValueAssignment attributedAssignment(
      String propertyUrn, String sourceUrn) {
    return new StructuredPropertyValueAssignment()
        .setPropertyUrn(UrnUtils.getUrn(propertyUrn))
        .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("testValue")))
        .setAttribution(
            new MetadataAttribution()
                .setSource(UrnUtils.getUrn(sourceUrn))
                .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"))
                .setTime(0L));
  }

  @Test
  public void testUnattributedRemoveDeletesAllEntriesForProperty() throws Exception {
    // (srcA, propX), (srcB, propX), (srcC, propY)
    StructuredProperties initial = new StructuredProperties();
    initial.setProperties(
        new StructuredPropertyValueAssignmentArray(
            attributedAssignment("urn:li:structuredProperty:propX", "urn:li:dataHubAction:srcA"),
            attributedAssignment("urn:li:structuredProperty:propX", "urn:li:dataHubAction:srcB"),
            attributedAssignment("urn:li:structuredProperty:propY", "urn:li:dataHubAction:srcC")));

    // Plain remove of propX — should delete the entire list at key propX.
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/properties/urn:li:structuredProperty:propX"))
                .build());

    StructuredProperties result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getProperties());
    List<String> propUrns =
        result.getProperties().stream()
            .map(p -> p.getPropertyUrn().toString())
            .collect(Collectors.toList());
    Assert.assertFalse(
        propUrns.contains("urn:li:structuredProperty:propX"), "all propX entries should be gone");
    Assert.assertTrue(
        propUrns.contains("urn:li:structuredProperty:propY"), "propY entry should survive");
    Assert.assertEquals(result.getProperties().size(), 1);
    StructuredPropertyValueAssignment survivor = result.getProperties().get(0);
    Assert.assertNotNull(survivor.getAttribution());
    Assert.assertEquals(
        survivor.getAttribution().getSource().toString(), "urn:li:dataHubAction:srcC");
  }

  @Test
  public void testAddTwoDistinctEntries() throws Exception {
    StructuredProperties initial = new StructuredProperties();
    initial.setProperties(new StructuredPropertyValueAssignmentArray());

    JsonPatch patchA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/properties/urn:li:structuredProperty:propA")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("propertyUrn", "urn:li:structuredProperty:propA")
                                        .add(
                                            "values",
                                            Json.createArrayBuilder()
                                                .add(
                                                    Json.createObjectBuilder()
                                                        .add("string", "valueA"))))))
                .build());

    JsonPatch patchB =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/properties/urn:li:structuredProperty:propB")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("propertyUrn", "urn:li:structuredProperty:propB")
                                        .add(
                                            "values",
                                            Json.createArrayBuilder()
                                                .add(
                                                    Json.createObjectBuilder()
                                                        .add("string", "valueB"))))))
                .build());

    StructuredProperties afterA = TEMPLATE.applyPatch(initial, patchA);
    StructuredProperties result = TEMPLATE.applyPatch(afterA, patchB);

    Assert.assertNotNull(result.getProperties());
    Assert.assertEquals(result.getProperties().size(), 2);
    List<String> propUrns =
        result.getProperties().stream()
            .map(p -> p.getPropertyUrn().toString())
            .collect(Collectors.toList());
    Assert.assertTrue(
        propUrns.contains("urn:li:structuredProperty:propA"), "propA should be present");
    Assert.assertTrue(
        propUrns.contains("urn:li:structuredProperty:propB"), "propB should be present");
  }

  @Test
  public void testRemoveOneOfTwoEntries() throws Exception {
    StructuredProperties initial = new StructuredProperties();
    initial.setProperties(
        new StructuredPropertyValueAssignmentArray(
            new StructuredPropertyValueAssignment()
                .setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:propA"))
                .setValues(
                    new PrimitivePropertyValueArray(PrimitivePropertyValue.create("valueA"))),
            new StructuredPropertyValueAssignment()
                .setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:propB"))
                .setValues(
                    new PrimitivePropertyValueArray(PrimitivePropertyValue.create("valueB")))));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/properties/urn:li:structuredProperty:propA"))
                .build());

    StructuredProperties result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getProperties());
    Assert.assertEquals(result.getProperties().size(), 1);
    List<String> propUrns =
        result.getProperties().stream()
            .map(p -> p.getPropertyUrn().toString())
            .collect(Collectors.toList());
    Assert.assertFalse(
        propUrns.contains("urn:li:structuredProperty:propA"), "propA should be removed");
    Assert.assertTrue(propUrns.contains("urn:li:structuredProperty:propB"), "propB should remain");
  }

  @Test
  public void testUnattributedAddToDuplicateKeyUpserts() throws Exception {
    // (srcA, propX), (srcB, propX) — plain add for propX replaces the whole list at that key.
    StructuredProperties initial = new StructuredProperties();
    initial.setProperties(
        new StructuredPropertyValueAssignmentArray(
            attributedAssignment("urn:li:structuredProperty:propX", "urn:li:dataHubAction:srcA"),
            attributedAssignment("urn:li:structuredProperty:propX", "urn:li:dataHubAction:srcB")));

    // Plain add of propX at the property level — replaces all entries for propX.
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/properties/urn:li:structuredProperty:propX")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("propertyUrn", "urn:li:structuredProperty:propX")
                                        .add(
                                            "values",
                                            Json.createArrayBuilder()
                                                .add(
                                                    Json.createObjectBuilder()
                                                        .add("string", "testValue"))))))
                .build());

    StructuredProperties result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getProperties());
    List<String> propUrns =
        result.getProperties().stream()
            .map(p -> p.getPropertyUrn().toString())
            .collect(Collectors.toList());
    // A plain add at the property level replaces the entire sub-map for propX.
    long propXCount = propUrns.stream().filter("urn:li:structuredProperty:propX"::equals).count();
    Assert.assertEquals(propXCount, 1L, "plain add at property level replaces all entries");
  }
}
