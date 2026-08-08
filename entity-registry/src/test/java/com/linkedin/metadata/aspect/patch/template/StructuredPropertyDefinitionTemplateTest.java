package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertyDefinitionPatchBuilder;
import com.linkedin.metadata.aspect.patch.template.structuredproperty.StructuredPropertyDefinitionTemplate;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredPropertyDefinition;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatch;
import jakarta.json.JsonValue;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StructuredPropertyDefinitionTemplateTest {

  private static final StructuredPropertyDefinitionTemplate TEMPLATE =
      new StructuredPropertyDefinitionTemplate();
  private static final String TEST_PROPERTY_URN =
      "urn:li:structuredProperty:io.acryl.classification";

  private static PropertyValue stringValue(String value) {
    PrimitivePropertyValue primitive = new PrimitivePropertyValue();
    primitive.setString(value);
    return new PropertyValue().setValue(primitive);
  }

  /**
   * Bridges the patch produced by the builder into a jakarta {@link JsonPatch} the template
   * applies, so the round-trip exercises {@code addAllowedValue}'s escaping and the template's
   * decode together rather than a hand-written path.
   */
  private static JsonPatch toJsonPatch(GenericJsonPatch genericJsonPatch) {
    JsonArrayBuilder ops = Json.createArrayBuilder();
    for (GenericJsonPatch.PatchOp op : genericJsonPatch.getPatch()) {
      JsonObjectBuilder node =
          Json.createObjectBuilder().add("op", op.getOp()).add("path", op.getPath());
      if (op.getValue() != null) {
        node.add("value", (JsonValue) op.getValue());
      }
      ops.add(node);
    }
    return Json.createPatch(ops.build());
  }

  /**
   * End-to-end: an allowed value containing a "/" produced by the builder must survive the full
   * applyPatch stack as a single, correctly-decoded entry. Before the RFC 6901 escaping fix the
   * unescaped slash split the JSON Pointer path and the entry was dropped/malformed.
   */
  @Test
  public void testAddAllowedValueWithSlashRoundTrips() throws Exception {
    StructuredPropertyDefinition initial = TEMPLATE.getDefault();
    // An existing allowed value so the template's array->map transform is active.
    initial.setAllowedValues(new PropertyValueArray(stringValue("Public")));

    StructuredPropertyDefinitionPatchBuilder builder =
        new StructuredPropertyDefinitionPatchBuilder();
    builder.urn(Urn.createFromString(TEST_PROPERTY_URN));
    builder.addAllowedValue(stringValue("SITS/eVision"));

    StructuredPropertyDefinition result =
        TEMPLATE.applyPatch(initial, toJsonPatch(builder.getJsonPatch()));

    Assert.assertNotNull(result.getAllowedValues());
    List<String> values =
        result.getAllowedValues().stream().map(v -> v.getValue().getString()).toList();
    Assert.assertTrue(
        values.contains("SITS/eVision"), "slash-bearing allowed value should survive applyPatch");
    Assert.assertTrue(values.contains("Public"), "existing allowed value should be retained");
    Assert.assertEquals(result.getAllowedValues().size(), 2, "no split or duplicate entries");
  }
}
