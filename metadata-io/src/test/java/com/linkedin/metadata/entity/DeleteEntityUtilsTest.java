package com.linkedin.metadata.entity;

import com.datahub.util.RecordUtils;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.grammar.PdlSchemaParser;
import com.linkedin.data.schema.resolver.DefaultDataSchemaResolver;
import com.linkedin.entity.Aspect;
import com.linkedin.schema.SchemaMetadata;
import junit.framework.TestCase;
import org.testng.annotations.Test;

public class DeleteEntityUtilsTest extends TestCase {

  /**
   * Tests that Aspect Processor deletes the entire struct if it no longer has any fields
   */
  @Test
  public void testEmptyStructRemoval() {
    final String value = "{\"key_a\": \"hello\"}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: optional string\n"
        + "}");

    final DataSchema schema = pdlSchemaParser.lookupName("simple_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_a"));

    assertFalse(updatedAspect.data().containsKey("key_a"));
    assertTrue(updatedAspect.data().isEmpty());
  }

  /**
   * Tests that Aspect Processor deletes & removes optional values from a struct.
   */
  @Test
  public void testOptionalFieldRemoval() {
    final String value = "{\"key_a\": \"hello\", \"key_b\": \"world\"}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: optional string\n"
        + "key_b: string\n"
        + "}");

    final DataSchema schema = pdlSchemaParser.lookupName("simple_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_a"));

    assertFalse(updatedAspect.data().containsKey("key_a"));
    assertTrue(updatedAspect.data().containsKey("key_b"));
    assertEquals("world", updatedAspect.data().get("key_b"));
  }

  /**
   * Tests that Aspect Processor does not delete a non-optional value from a struct.
   */
  @Test
  public void testNonOptionalFieldRemoval() {
    final String value = "{\"key_a\": \"hello\", \"key_b\": \"world\"}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: string\n"
        + "key_b: string\n"
        + "}");

    final DataSchema schema = pdlSchemaParser.lookupName("simple_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_a"));

    assertTrue(updatedAspect.data().containsKey("key_a"));
    assertEquals("hello", updatedAspect.data().get("key_a"));
    assertTrue(updatedAspect.data().containsKey("key_b"));
    assertEquals("world", updatedAspect.data().get("key_b"));
    assertEquals(aspect, updatedAspect);
  }

  /**
   * Tests that Aspect Processor does not delete a non-optional value from a record referenced by another record.
   */
  @Test
  public void testNestedFieldRemoval() {
    final String value = "{\"key_c\": {\"key_a\": \"hello\", \"key_b\": \"world\"}}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: string\n"
        + "key_b: string\n"
        + "}");

    pdlSchemaParser.parse("record complex_record {\n"
        + "key_c: simple_record\n"
        + "}");

    final DataSchema schema = pdlSchemaParser.lookupName("complex_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_c", "key_a"));

    assertTrue(updatedAspect.data().containsKey("key_c"));
    assertEquals(aspect.data().get("key_c"), updatedAspect.data().get("key_c"));
  }

  /**
   * Tests that Aspect Processor is able to delete an optional sub-field while preserving nested structs.
   */
  @Test
  public void testOptionalNestedFieldRemoval() {
    final String value = "{\"key_c\": {\"key_a\": \"hello\", \"key_b\": \"world\"}}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: optional string\n"
        + "key_b: string\n"
        + "}");

    pdlSchemaParser.parse("record complex_record {\n"
        + "key_c: simple_record\n"
        + "}");

    final DataSchema schema = pdlSchemaParser.lookupName("complex_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_c", "key_a"));

    assertTrue(updatedAspect.data().containsKey("key_c"));
    assertNotSame(aspect.data().get("key_c"), updatedAspect.data().get("key_c"));

    // key_a should have been removed but not key_b
    assertFalse(((DataMap) updatedAspect.data().get("key_c")).containsKey("key_a"));
    assertTrue(((DataMap) updatedAspect.data().get("key_c")).containsKey("key_b"));
  }

  /**
   * Tests that the Aspect Processor will delete an entire struct if after removal of a field, it becomes empty &
   * is optional at some higher level.
   */
  @Test
  public void testRemovalOptionalFieldWithNonOptionalSubfield() {
    final String value = "{\"key_c\": {\"key_b\": \"world\"}}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: optional string\n"
        + "key_b: string\n"
        + "}");

    pdlSchemaParser.parse("record complex_record {\n"
        + "key_c: optional simple_record\n"
        + "}");

    final DataSchema schema = pdlSchemaParser.lookupName("complex_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("world", aspect, schema,
        new PathSpec("key_c", "key_b"));

    assertFalse(updatedAspect.data().containsKey("key_c"));
  }

  @Test
  public void testRemovalFromSingleArray() {
    final String value = "{\"key_a\": [\"hello\"]}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: array[string]\n"
        + "}");

    assertEquals(1, ((DataList) aspect.data().get("key_a")).size());

    final DataSchema schema = pdlSchemaParser.lookupName("simple_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_a", "*"));

    assertTrue(updatedAspect.data().containsKey("key_a"));
    assertTrue(((DataList) updatedAspect.data().get("key_a")).isEmpty());
  }

  @Test
  public void testRemovalFromMultipleArray() {
    final String value = "{\"key_a\": [\"hello\", \"world\"]}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: array[string]\n"
        + "}");

    assertEquals(2, ((DataList) aspect.data().get("key_a")).size());

    final DataSchema schema = pdlSchemaParser.lookupName("simple_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_a", "*"));

    assertTrue(updatedAspect.data().containsKey("key_a"));
    assertEquals(1, ((DataList) updatedAspect.data().get("key_a")).size());
    assertEquals("world", ((DataList) updatedAspect.data().get("key_a")).get(0));
  }

  /**
   * Tests that Aspect Processor is able to remove sub-field from array field while preserving nested structs.
   */
  @Test
  public void testRemovalNestedFieldFromArray() {
    final String value = "{\"key_c\": [{\"key_a\": \"hello\", \"key_b\": \"world\"}, {\"key_b\": \"extra info\"}]}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: optional string\n"
        + "key_b: string\n"
        + "}");

    pdlSchemaParser.parse("record complex_record {\n"
        + "key_c: array[simple_record]\n"
        + "}");

    assertEquals(2, ((DataList) aspect.data().get("key_c")).size());

    final DataSchema schema = pdlSchemaParser.lookupName("complex_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_c", "*", "key_a"));

    assertTrue(updatedAspect.data().containsKey("key_c"));
    assertEquals(2, ((DataList) updatedAspect.data().get("key_c")).size());

    assertNotSame(aspect.data().get("key_c"), updatedAspect.data().get("key_c"));

    // key_a field from first element from key_c should have been removed
    assertFalse(((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(0)).containsKey("key_a"));
    assertTrue(((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(0)).containsKey("key_b"));
    assertTrue(((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(1)).containsKey("key_b"));
    assertEquals("world", ((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(0)).get("key_b"));
    assertEquals("extra info", ((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(1)).get("key_b"));
  }

  /**
   * Tests that Aspect Processor is able to remove element from array field.
   */
  @Test
  public void testRemovalElementFromArray() {
    final String value = "{\"key_c\": [{\"key_a\": \"hello\"}, {\"key_b\": \"extra info\"}]}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: optional string\n"
        + "key_b: optional string\n"
        + "}");

    pdlSchemaParser.parse("record complex_record {\n"
        + "key_c: array[simple_record]\n"
        + "}");

    assertEquals(2, ((DataList) aspect.data().get("key_c")).size());

    final DataSchema schema = pdlSchemaParser.lookupName("complex_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_c", "*", "key_a"));

    assertTrue(updatedAspect.data().containsKey("key_c"));
    assertEquals(2, ((DataList) updatedAspect.data().get("key_c")).size());

    // First element from key_c should have been emptied
    assertFalse(((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(0)).containsKey("key_a"));
    assertTrue(((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(0)).isEmpty());
    assertTrue(((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(1)).containsKey("key_b"));
    assertEquals("extra info", ((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(1)).get("key_b"));
  }

  /**
   * Tests that Aspect Processor removes array if empty when removing underlying structs
   */
  @Test
  public void testRemovalEmptyArray() {
    final String value = "{\"key_c\": [{\"key_a\": \"hello\"}]}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: optional string\n"
        + "key_b: optional string\n"
        + "}");

    pdlSchemaParser.parse("record complex_record {\n"
        + "key_c: array[simple_record]\n"
        + "}");

    assertEquals(1, ((DataList) aspect.data().get("key_c")).size());

    final DataSchema schema = pdlSchemaParser.lookupName("complex_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_c", "*", "key_a"));

    assertTrue(updatedAspect.data().containsKey("key_c"));
    assertEquals(1, ((DataList) updatedAspect.data().get("key_c")).size());
    assertTrue(((DataMap) ((DataList) updatedAspect.data().get("key_c")).get(0)).isEmpty());
  }

  /**
   * Tests that Aspect Processor removes optional array field from struct when it is empty
   */
  @Test
  public void testRemovalOptionalEmptyArray() {
    final String value = "{\"key_c\": [{\"key_a\": \"hello\"}]}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record {\n"
        + "key_a: string\n"
        + "key_b: optional string\n"
        + "}");

    pdlSchemaParser.parse("record complex_record {\n"
        + "key_c: optional array[simple_record]\n"
        + "}");

    assertEquals(1, ((DataList) aspect.data().get("key_c")).size());

    final DataSchema schema = pdlSchemaParser.lookupName("complex_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_c", "*", "key_a"));

    // contains an empty key_c
    assertTrue(updatedAspect.data().containsKey("key_c"));
    assertTrue(((DataList) updatedAspect.data().get("key_c")).isEmpty());
  }

  /**
   * Tests that Aspect Processor removes nested structs more than 1 level deep from an optional field.
   */
  @Test
  public void testNestedNonOptionalSubFieldsOnOptionalField() {
    final String value = "{\"key_c\": {\"key_b\": {\"key_a\": \"hello\"}}}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final PdlSchemaParser pdlSchemaParser = new PdlSchemaParser(new DefaultDataSchemaResolver());
    pdlSchemaParser.parse("record simple_record_1 {\n"
        + "key_a: string\n"
        + "}");

    pdlSchemaParser.parse("record simple_record_2 {\n"
        + "key_b: simple_record_1\n"
        + "}");

    pdlSchemaParser.parse("record complex_record {\n"
        + "key_c: optional simple_record_2\n"
        + "}");

    assertTrue(aspect.data().containsKey("key_c"));

    final DataSchema schema = pdlSchemaParser.lookupName("complex_record");
    final Aspect updatedAspect = DeleteEntityUtils.getAspectWithReferenceRemoved("hello", aspect, schema,
        new PathSpec("key_c", "key_b", "key_a"));

    assertFalse(updatedAspect.data().containsKey("key_c"));
  }

  /**
   * Tests that the aspect processor is able to remove fields that are deeply nested where the top-level field is
   * optional.
   * This example is based on the SchemaMetadata object.
   */
  @Test
  public void testSchemaMetadataDelete() {
    final String value = "{\"fields\": [{\"globalTags\": {\"tags\": [{\"tag\": \"urn:li:tag:Dimension\"}]}}]}";
    final Aspect aspect = RecordUtils.toRecordTemplate(Aspect.class, value);

    final Aspect updatedAspect =
        DeleteEntityUtils.getAspectWithReferenceRemoved("urn:li:tag:Dimension", aspect, SchemaMetadata.dataSchema(),
            new PathSpec("fields", "*", "globalTags", "tags", "*", "tag"));

    assertFalse(updatedAspect.data().toString().contains("urn:li:tag:Dimension"));
    assertTrue(updatedAspect.data().containsKey("fields"));
    // tags must be empty, not field
    assertEquals(1, ((DataList) updatedAspect.data().get("fields")).size());
    assertEquals(0, ((DataList) ((DataMap) ((DataMap) ((DataList) updatedAspect.data().get("fields")).get(0))
        .get("globalTags")).get("tags")).size());
  }
}