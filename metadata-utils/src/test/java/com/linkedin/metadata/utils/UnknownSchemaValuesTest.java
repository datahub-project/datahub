package com.linkedin.metadata.utils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UnknownSchemaValuesTest {

  private static final RecordDataSchema SCHEMA =
      (RecordDataSchema)
          DataTemplateUtil.parseSchema(
              "{ \"type\": \"record\", \"name\": \"Values\", \"namespace\": \"test\", \"fields\": ["
                  + " { \"name\": \"kind\", \"type\": { \"type\": \"enum\", \"name\": \"Kind\","
                  + "   \"symbols\": [ \"A\", \"B\" ] } },"
                  + " { \"name\": \"value\", \"optional\": true, \"type\": [ \"string\", \"double\" ] },"
                  + " { \"name\": \"count\", \"optional\": true, \"type\": \"long\" } ] }");

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlings.reset();
  }

  @Test
  public void testEnumSymbolOrUnionMemberFromANewerVersion() {
    assertTrue(UnknownSchemaValues.failsOnlyOnUnknownValues(record(Map.of("kind", "C"))));
    assertTrue(
        UnknownSchemaValues.failsOnlyOnUnknownValues(
            record(Map.of("kind", "A", "value", new DataMap(Map.of("boolean", true))))));
    assertTrue(
        UnknownSchemaValues.failsOnlyOnUnknownValues(
            record(Map.of("kind", "C", "value", new DataMap(Map.of("boolean", true))))));
  }

  @Test
  public void testAnyOtherErrorIsARealError() {
    // A wrong type next to an unknown symbol: not explained by a newer version.
    assertFalse(
        UnknownSchemaValues.failsOnlyOnUnknownValues(record(Map.of("kind", "C", "count", "x"))));
    assertFalse(UnknownSchemaValues.failsOnlyOnUnknownValues(record(Map.of("count", 1L))));
  }

  @Test
  public void testValidAspectAndUnrecognizedFieldsAreNotFailures() {
    assertFalse(UnknownSchemaValues.failsOnlyOnUnknownValues(record(Map.of("kind", "A"))));
    // Unrecognized fields are trimmed on read and write, so they don't fail validation.
    assertFalse(
        UnknownSchemaValues.failsOnlyOnUnknownValues(
            record(Map.of("kind", "A", "fieldFromNewerBuild", "x"))));
    assertFalse(UnknownSchemaValues.failsOnlyOnUnknownValues((ValidationResult) null));
  }

  @Test
  public void testCallersAspectIsNotTrimmed() {
    RecordTemplate aspect = record(Map.of("kind", "C", "fieldFromNewerBuild", "x"));

    UnknownSchemaValues.failsOnlyOnUnknownValues(aspect);

    assertEquals(aspect.data().get("fieldFromNewerBuild"), "x");
  }

  @Test
  public void testOnlyTheEnumFlagAttributesThemToANewerVersion() {
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec(anyString())).thenReturn(Optional.empty());
    RecordTemplate aspect = record(Map.of("kind", "C"));

    assertTrue(
        TolerantHandling.of(false, true, entityType -> true)
            .isFromNewerVersion(aspect, null, null, registry));
    assertFalse(
        TolerantHandling.of(true, false, entityType -> true)
            .isFromNewerVersion(aspect, null, null, registry));
  }

  @Test
  public void testOnlyTolerantHandlingAttributesThemToANewerVersion() {
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec(anyString())).thenReturn(Optional.empty());
    RecordTemplate aspect = record(Map.of("kind", "C"));

    assertTrue(TolerantHandling.INSTANCE.isFromNewerVersion(aspect, null, null, registry));
    assertFalse(StrictHandling.INSTANCE.isFromNewerVersion(aspect, null, null, registry));
    assertFalse(
        TolerantHandling.INSTANCE.isFromNewerVersion(
            record(Map.of("kind", "A")), null, null, registry));
  }

  private static RecordTemplate record(Map<String, Object> fields) {
    return new RecordTemplate(new DataMap(fields), SCHEMA) {};
  }
}
