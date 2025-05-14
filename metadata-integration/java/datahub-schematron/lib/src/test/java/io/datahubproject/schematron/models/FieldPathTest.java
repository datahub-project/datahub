package io.datahubproject.schematron.models;

import static org.testng.Assert.*;

import com.linkedin.schema.ArrayType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.UnionType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.*;

@Test(groups = "unit")
public class FieldPathTest {

  @Test(groups = "basic")
  public void testEmptyFieldPath() {
    FieldPath path = new FieldPath();
    assertEquals(path.asString(), "[version=2.0]");
  }

  @Test(groups = "basic")
  public void testKeySchemaPath() {
    FieldPath path = new FieldPath();
    path.setKeySchema(true);
    assertEquals(path.asString(), "[version=2.0].[key=True]");
  }

  @Test(groups = "basic")
  public void testSimplePath() {
    FieldPath path = new FieldPath();
    FieldElement element =
        new FieldElement(
            Collections.singletonList("string"), Collections.singletonList("schema"), "name", null);
    path.setPath(Collections.singletonList(element));
    assertEquals(path.asString(), "[version=2.0].[type=string].name");
  }

  @Test(groups = "nested")
  public void testNestedPath() {
    FieldPath path = new FieldPath();
    FieldElement record =
        new FieldElement(
            Collections.singletonList("record"),
            Collections.singletonList("record-schema"),
            "user",
            null);
    FieldElement field =
        new FieldElement(
            Collections.singletonList("string"),
            Collections.singletonList("string-schema"),
            "name",
            null);
    path.setPath(Arrays.asList(record, field));
    assertEquals(path.asString(), "[version=2.0].[type=record].user.[type=string].name");
  }

  @Test(groups = "complex")
  public void testUnionPath() {
    FieldPath path = new FieldPath();

    // Add union type
    FieldElement union =
        new FieldElement(
            Collections.singletonList("union"),
            Collections.singletonList("union-schema"),
            "document",
            null);

    // Add specific union member (record type)
    FieldElement passport =
        new FieldElement(
            Collections.singletonList("Passport"),
            Collections.singletonList("passport-schema"),
            "document",
            new DataHubType(UnionType.class, "Passport"));

    // Add field within the record
    FieldElement number =
        new FieldElement(
            Collections.singletonList("string"),
            Collections.singletonList("string-schema"),
            "number",
            null);

    path.setPath(Arrays.asList(union, passport, number));
    assertEquals(
        path.asString(),
        "[version=2.0].[type=union].document.[type=Passport].document.[type=string].number");
  }

  @Test(groups = "operations")
  public void testClonePlus() {
    FieldPath original = new FieldPath();
    FieldElement element1 =
        new FieldElement(
            Collections.singletonList("record"),
            Collections.singletonList("schema1"),
            "user",
            null);
    original.setPath(Collections.singletonList(element1));

    FieldElement element2 =
        new FieldElement(
            Collections.singletonList("string"),
            Collections.singletonList("schema2"),
            "name",
            null);

    FieldPath newPath = original.clonePlus(element2);

    // Verify original path remains unchanged
    assertEquals(original.asString(), "[version=2.0].[type=record].user");

    // Verify new path has both elements
    assertEquals(newPath.asString(), "[version=2.0].[type=record].user.[type=string].name");
  }

  @Test(groups = "operations")
  public void testExpandType() {
    FieldPath path = new FieldPath();
    FieldElement element = new FieldElement(new ArrayList<>(), new ArrayList<>(), "field", null);
    path.setPath(Collections.singletonList(element));

    FieldPath expanded = path.expandType("string", "schema");

    assertEquals(expanded.asString(), "[version=2.0].[type=string].field");
    assertEquals(expanded.getPath().get(0).getType().size(), 1);
    assertEquals(expanded.getPath().get(0).getType().get(0), "string");
    assertEquals(expanded.getPath().get(0).getSchemaTypes().get(0), "schema");
  }

  @Test(groups = "operations")
  public void testHasFieldName() {
    FieldPath path = new FieldPath();
    assertFalse(path.hasFieldName());

    FieldElement element =
        new FieldElement(
            Collections.singletonList("string"), Collections.singletonList("schema"), "name", null);
    path.setPath(Collections.singletonList(element));
    assertTrue(path.hasFieldName());
  }

  @Test(groups = "operations")
  public void testEnsureFieldName() {
    FieldPath path = new FieldPath();
    assertFalse(path.hasFieldName());

    path.ensureFieldName();
    assertTrue(path.hasFieldName());
    assertEquals(path.getPath().get(0).getName(), FieldPath.EMPTY_FIELD_NAME);
  }

  @Test(groups = "complex")
  public void testArrayPath() {
    FieldPath path = new FieldPath();
    FieldElement array =
        new FieldElement(
            Collections.singletonList("array"),
            Collections.singletonList("array-schema"),
            "items",
            new DataHubType(ArrayType.class, "string"));

    path.setPath(Collections.singletonList(array));
    assertEquals(path.asString(), "[version=2.0].[type=array].items");
  }

  @Test(groups = "complex")
  public void testMapPath() {
    FieldPath path = new FieldPath();
    FieldElement map =
        new FieldElement(
            Collections.singletonList("map"),
            Collections.singletonList("map-schema"),
            "properties",
            new DataHubType(MapType.class, "string"));

    path.setPath(Collections.singletonList(map));
    assertEquals(path.asString(), "[version=2.0].[type=map].properties");
  }

  @Test(groups = "complex")
  public void testMultipleTypesInPath() {
    FieldPath path = new FieldPath();
    FieldElement element =
        new FieldElement(
            Arrays.asList("union", "string"),
            Arrays.asList("union-schema", "string-schema"),
            "field",
            null);
    path.setPath(Collections.singletonList(element));
    assertEquals(path.asString(), "[version=2.0].[type=union].[type=string].field");
  }

  @Test(groups = "complex")
  public void testParentTypeHandling() {
    FieldPath path = new FieldPath();
    DataHubType parentType = new DataHubType(ArrayType.class, "string");
    FieldElement element =
        new FieldElement(
            Collections.singletonList("array"),
            Collections.singletonList("array-schema"),
            "items",
            parentType);
    path.setPath(Collections.singletonList(element));

    assertNotNull(path.getPath().get(0).getParentType());
    assertEquals(path.getPath().get(0).getParentType().getType(), ArrayType.class);
    assertEquals(path.getPath().get(0).getParentType().getNestedType(), "string");
  }

  @Test(groups = "edge-cases")
  public void testNoParentPath() {
    FieldPath path = new FieldPath();
    assertEquals(path.asString(), "[version=2.0]");
  }

  @Test(groups = "edge-cases")
  public void testEmptyElementList() {
    FieldPath path = new FieldPath();
    path.setPath(new ArrayList<>());
    assertEquals(path.asString(), "[version=2.0]");
  }

  @DataProvider(name = "invalidPaths")
  public Object[][] getInvalidPaths() {
    return new Object[][] {
      {null, "Expected IllegalArgumentException for null element"},
      {
        Arrays.asList((FieldElement) null),
        "Expected IllegalArgumentException for null element in list"
      }
    };
  }

  @Test(
      groups = "edge-cases",
      dataProvider = "invalidPaths",
      expectedExceptions = IllegalArgumentException.class)
  public void testInvalidPaths(List<FieldElement> elements, String message) {
    FieldPath path = new FieldPath();
    path.setPath(elements);
  }
}
