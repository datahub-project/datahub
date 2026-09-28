package com.linkedin.datahub.graphql.resolvers.logical;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.datahub.graphql.generated.EditableSchemaFieldInput;
import com.linkedin.datahub.graphql.generated.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import java.util.List;
import org.testng.annotations.Test;

public class SchemaMetadataUtilsTest {

  private static final DataPlatformUrn PLATFORM = new DataPlatformUrn("logical");

  private EditableSchemaFieldInput col(String path, SchemaFieldDataType type) {
    EditableSchemaFieldInput c = new EditableSchemaFieldInput();
    c.setFieldPath(path);
    c.setType(type);
    return c;
  }

  private SchemaMetadata schema(List<EditableSchemaFieldInput> cols) {
    return SchemaMetadataUtils.buildSchemaMetadata("s", PLATFORM, cols);
  }

  @Test
  public void testBuildSchemaMetadataMapsTypesAndFields() {
    EditableSchemaFieldInput idCol = new EditableSchemaFieldInput();
    idCol.setFieldPath("id");
    idCol.setType(SchemaFieldDataType.NUMBER);

    EditableSchemaFieldInput nameCol = new EditableSchemaFieldInput();
    nameCol.setFieldPath("name");
    nameCol.setType(SchemaFieldDataType.STRING);

    SchemaMetadata schema =
        SchemaMetadataUtils.buildSchemaMetadata(
            "my_domain.my_table", new DataPlatformUrn("logical"), List.of(idCol, nameCol));

    assertEquals(schema.getFields().size(), 2);
    assertEquals(schema.getFields().get(0).getFieldPath(), "id");
    // nativeDataType is derived from the canonical type name (no user input)
    assertEquals(schema.getFields().get(0).getNativeDataType(), "NUMBER");
    assertTrue(schema.getFields().get(0).getType().getType().isNumberType());
    assertTrue(schema.getFields().get(1).getType().getType().isStringType());
    assertEquals(schema.getFields().get(1).getNativeDataType(), "STRING");
  }

  @Test
  public void testValidateColumnsRejectsEmpty() {
    assertThrows(
        IllegalArgumentException.class, () -> SchemaMetadataUtils.validateColumns(List.of()));
  }

  @Test
  public void testValidateColumnsRejectsDuplicateFieldPaths() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SchemaMetadataUtils.validateColumns(
                List.of(
                    col("id", SchemaFieldDataType.NUMBER), col("id", SchemaFieldDataType.STRING))));
  }

  @Test
  public void testValidateColumnsAcceptsUniqueNonEmpty() {
    SchemaMetadataUtils.validateColumns(
        List.of(col("id", SchemaFieldDataType.NUMBER), col("name", SchemaFieldDataType.STRING)));
  }

  @Test
  public void testAddColumnIsNotBreaking() {
    SchemaMetadata existing = schema(List.of(col("id", SchemaFieldDataType.NUMBER)));
    SchemaMetadata updated =
        schema(
            List.of(
                col("id", SchemaFieldDataType.NUMBER), col("name", SchemaFieldDataType.STRING)));
    assertFalse(SchemaMetadataUtils.isBreakingChange(existing, updated));
  }

  @Test
  public void testRemoveColumnIsBreaking() {
    SchemaMetadata existing =
        schema(
            List.of(
                col("id", SchemaFieldDataType.NUMBER), col("name", SchemaFieldDataType.STRING)));
    SchemaMetadata updated = schema(List.of(col("id", SchemaFieldDataType.NUMBER)));
    assertTrue(SchemaMetadataUtils.isBreakingChange(existing, updated));
  }

  @Test
  public void testRenameColumnIsBreaking() {
    SchemaMetadata existing = schema(List.of(col("id", SchemaFieldDataType.NUMBER)));
    SchemaMetadata updated = schema(List.of(col("identifier", SchemaFieldDataType.NUMBER)));
    assertTrue(SchemaMetadataUtils.isBreakingChange(existing, updated));
  }

  @Test
  public void testRetypeColumnIsBreaking() {
    SchemaMetadata existing = schema(List.of(col("id", SchemaFieldDataType.NUMBER)));
    SchemaMetadata updated = schema(List.of(col("id", SchemaFieldDataType.STRING)));
    assertTrue(SchemaMetadataUtils.isBreakingChange(existing, updated));
  }

  @Test
  public void testReorderIsNotBreaking() {
    SchemaMetadata existing =
        schema(
            List.of(
                col("id", SchemaFieldDataType.NUMBER), col("name", SchemaFieldDataType.STRING)));
    SchemaMetadata updated =
        schema(
            List.of(
                col("name", SchemaFieldDataType.STRING), col("id", SchemaFieldDataType.NUMBER)));
    assertFalse(SchemaMetadataUtils.isBreakingChange(existing, updated));
  }
}
