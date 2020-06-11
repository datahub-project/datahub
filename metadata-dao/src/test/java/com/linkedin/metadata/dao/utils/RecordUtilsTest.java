package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.validator.InvalidSchemaException;
import com.linkedin.metadata.validator.ValidationUtils;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectBaz;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntitySnapshot;
import com.linkedin.testing.singleaspectentity.EntityValue;
import java.io.IOException;
import java.util.Arrays;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class RecordUtilsTest {

  @Test
  public void testToJsonString() throws IOException {
    AspectFoo foo = new AspectFoo().setValue("foo");
    String expected =
        loadJsonFromResource("foo.json").replaceAll("\\s+", "").replaceAll("\\n", "").replaceAll("\\r", "");

    String actual = RecordUtils.toJsonString(foo);

    assertEquals(actual, expected);
  }

  @Test
  public void testToRecordTemplate() throws IOException {
    AspectFoo expected = new AspectFoo().setValue("foo");
    String jsonString = loadJsonFromResource("foo.json");

    AspectFoo actual = RecordUtils.toRecordTemplate(AspectFoo.class, jsonString);

    assertEquals(actual, expected);

    RecordTemplate actual2 = RecordUtils.toRecordTemplate("com.linkedin.testing.AspectFoo", expected.data());

    assertEquals(actual2.getClass(), AspectFoo.class);
    assertEquals(actual2, expected);
  }

  @Test(expectedExceptions = ModelConversionException.class)
  public void testToRecordTemplateFromInvalidString() {
    RecordUtils.toRecordTemplate(AspectFoo.class, "invalid_json");
  }

  @Test
  public void testGetValidRecordDataSchemaField() {
    RecordDataSchema schema = ValidationUtils.getRecordSchema(AspectFoo.class);
    RecordDataSchema.Field expected = schema.getField("value");

    assertEquals(RecordUtils.getRecordDataSchemaField(new AspectFoo().setValue("foo"), "value"), expected);
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testGetInvalidRecordDataSchemaField() {
    RecordUtils.getRecordDataSchemaField(new AspectFoo().setValue("foo"), "non-existing-field");
  }

  @Test
  public void testSetRecordTemplatePrimitiveField() {
    AspectBaz baz = new AspectBaz();

    RecordUtils.setRecordTemplatePrimitiveField(baz, "boolField", Boolean.FALSE);
    RecordUtils.setRecordTemplatePrimitiveField(baz, "stringField", "baz");
    RecordUtils.setRecordTemplatePrimitiveField(baz, "longField", Long.valueOf(1234L));

    assertFalse(baz.isBoolField());
    assertEquals(baz.getStringField(), "baz");
    assertEquals(baz.getLongField(), Long.valueOf(1234L));
  }

  @Test
  public void testSetRecordTemplateComplexField() throws IOException {
    AspectBaz baz = new AspectBaz();

    StringArray stringArray = new StringArray(Arrays.asList("1", "2", "3"));
    RecordUtils.setRecordTemplateComplexField(baz, "arrayField", stringArray);

    AspectFoo foo = new AspectFoo().setValue("foo");
    RecordUtils.setRecordTemplateComplexField(baz, "recordField", foo);

    assertEquals(baz.getArrayField(), stringArray);
    assertEquals(baz.getRecordField(), foo);
  }

  @Test
  public void testGetRecordTemplatePrimitiveField() throws IOException {
    AspectBaz baz = loadAspectBaz("baz.json");

    assertTrue(RecordUtils.getRecordTemplateField(baz, "boolField", Boolean.class));
    assertEquals(RecordUtils.getRecordTemplateField(baz, "stringField", String.class), "baz");
    assertEquals(RecordUtils.getRecordTemplateField(baz, "longField", Long.class), Long.valueOf(1234L));
  }

  @Test
  public void testGetRecordTemplateUrnField() {
    Urn urn = makeUrn(1);
    EntitySnapshot snapshot = new EntitySnapshot().setUrn(urn);

    assertEquals(RecordUtils.getRecordTemplateField(snapshot, "urn", Urn.class), urn);
  }

  @Test
  public void testGetRecordTemplateWrappedField() throws IOException {
    AspectBaz baz = loadAspectBaz("baz.json");

    StringArray stringArray = RecordUtils.getRecordTemplateWrappedField(baz, "arrayField", StringArray.class);

    assertEquals(stringArray.toArray(), new String[]{"1", "2", "3"});
  }

  @Test
  public void testGetSelectedRecordTemplateFromUnion() throws IOException {
    AspectBaz baz = new AspectBaz();
    baz.setUnionField(new AspectBaz.UnionField());
    baz.getUnionField().setAspectFoo(new AspectFoo().setValue("foo"));

    RecordTemplate selected = RecordUtils.getSelectedRecordTemplateFromUnion(baz.getUnionField());

    assertEquals(selected.getClass(), AspectFoo.class);
  }

  @Test
  public void testSetSelectedRecordTemplateInUnion() throws IOException {
    AspectBaz baz = new AspectBaz();
    baz.setUnionField(new AspectBaz.UnionField());
    AspectFoo expected = new AspectFoo().setValue("foo");

    RecordUtils.setSelectedRecordTemplateInUnion(baz.getUnionField(), expected);

    assertEquals(baz.getUnionField().getAspectFoo(), expected);
  }

  @Test
  public void testGetValidMetadataSnapshotClassFromName() {
    Class<? extends RecordTemplate> actualClass =
        ModelUtils.getMetadataSnapshotClassFromName("com.linkedin.testing.EntitySnapshot");
    assertEquals(actualClass, EntitySnapshot.class);
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testGetInvalidMetadataSnapshotClassFromName() {
    ModelUtils.getMetadataSnapshotClassFromName("com.linkedin.testing.AspectInvalid");
  }

  @Test
  public void testExtractAspectFromSingleAspectEntity() {
    String field1 = "foo";
    EntityValue value = new EntityValue();
    value.setValue(field1);

    AspectBar aspect = new AspectBar();
    aspect.setValue(field1);

    assertEquals(RecordUtils.extractAspectFromSingleAspectEntity(value, AspectBar.class), aspect);
  }

  private AspectBaz loadAspectBaz(String resourceName) throws IOException {
    return RecordUtils.toRecordTemplate(AspectBaz.class, loadJsonFromResource(resourceName));
  }
}
