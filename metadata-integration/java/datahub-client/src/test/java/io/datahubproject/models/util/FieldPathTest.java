package io.datahubproject.models.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class FieldPathTest {

  @Test
  public void testSimplePathConstruction() {
    FieldPath path = new FieldPath("field1.field2.field3");
    assertEquals("field1.field2.field3", path.simplePath());
    assertEquals("1", path.version());
    assertEquals("field3", path.leafFieldName());
    assertEquals(3, path.depth());
    assertFalse(path.isTopLevel());
  }

  @Test
  public void testTopLevelPath() {
    FieldPath path = new FieldPath("singleField");
    assertTrue(path.isTopLevel());
    assertEquals(1, path.depth());
    assertEquals("singleField", path.leafFieldName());
    assertEquals("singleField", path.simplePath());
  }

  @Test
  public void testVersionExtraction() {
    FieldPath path = new FieldPath("[version=2.0].field1.field2");
    assertEquals("2.0", path.version());
    assertEquals("field1.field2", path.simplePath());
  }

  @Test
  public void testComplexPathWithMetadata() {
    FieldPath path =
        new FieldPath("[version=2.0][type=Foo].address.[type=union].[type=string].street");
    assertEquals("2.0", path.version());
    assertEquals("address.street", path.simplePath());
    assertEquals("street", path.leafFieldName());
    assertEquals(2, path.depth());
  }

  @Test
  public void testPathWithMultipleMetadataBlocks() {
    FieldPath path =
        new FieldPath("[version=3][type=Record][nullable=true].user.details[type=union].name");
    System.out.println(path.simplePath());
    assertEquals("3", path.version());
    assertEquals("user.details.name", path.simplePath());
    assertEquals("name", path.leafFieldName());
    assertEquals(3, path.depth());
  }

  @Test
  public void testPathWithTrailingMetadata() {
    FieldPath path = new FieldPath("field1.field2[type=string]");
    assertEquals("field1.field2", path.simplePath());
    assertEquals("1", path.version());
  }

  @Test
  public void testPathWithOnlyMetadata() {
    FieldPath path = new FieldPath("[type=string][version=4]");
    assertEquals("", path.simplePath());
    assertEquals("4", path.version());
  }

  @Test
  public void testDepthCalculationWithComplexPath() {
    FieldPath path =
        new FieldPath("[version=2].user.[type=record].address.[type=union].[type=string].street");
    assertEquals(3, path.depth());
    assertEquals("user.address.street", path.simplePath());
  }

  @Test
  public void testLeafFieldNameWithMetadata() {
    FieldPath path = new FieldPath("field1.field2[type=string]");
    assertEquals("field2[type=string]", path.leafFieldName());
  }
}
