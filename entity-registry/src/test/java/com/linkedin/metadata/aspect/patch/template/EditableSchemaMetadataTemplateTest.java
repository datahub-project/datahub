package com.linkedin.metadata.aspect.patch.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.aspect.patch.template.dataset.EditableSchemaMetadataTemplate;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import jakarta.json.JsonPatchBuilder;
import org.testng.annotations.Test;

public class EditableSchemaMetadataTemplateTest {

  private static final EditableSchemaMetadataTemplate TEMPLATE =
      new EditableSchemaMetadataTemplate();

  @Test
  public void testAddTagToNewFieldPreservesFieldPath() throws Exception {
    EditableSchemaMetadata initial = TEMPLATE.getDefault();

    JsonPatchBuilder builder = Json.createPatchBuilder();
    builder.add(
        "/editableSchemaFieldInfo/field1/globalTags/tags/urn:li:tag:tag1",
        Json.createObjectBuilder().add("tag", "urn:li:tag:tag1").build());
    JsonPatch patch = builder.build();

    EditableSchemaMetadata result = TEMPLATE.applyPatch(initial, patch);

    assertNotNull(result.getEditableSchemaFieldInfo());
    assertEquals(result.getEditableSchemaFieldInfo().size(), 1);
    EditableSchemaFieldInfo fieldInfo = result.getEditableSchemaFieldInfo().get(0);
    assertEquals(fieldInfo.getFieldPath(), "field1");
    assertNotNull(fieldInfo.getGlobalTags());
    assertEquals(fieldInfo.getGlobalTags().getTags().size(), 1);
    assertEquals(fieldInfo.getGlobalTags().getTags().get(0).getTag().toString(), "urn:li:tag:tag1");
  }

  @Test
  public void testAddTermToNewFieldPreservesFieldPath() throws Exception {
    EditableSchemaMetadata initial = TEMPLATE.getDefault();

    JsonPatchBuilder builder = Json.createPatchBuilder();
    builder.add(
        "/editableSchemaFieldInfo/field2/glossaryTerms/terms/urn:li:glossaryTerm:term1",
        Json.createObjectBuilder().add("urn", "urn:li:glossaryTerm:term1").build());
    JsonPatch patch = builder.build();

    EditableSchemaMetadata result = TEMPLATE.applyPatch(initial, patch);

    assertNotNull(result.getEditableSchemaFieldInfo());
    assertEquals(result.getEditableSchemaFieldInfo().size(), 1);
    EditableSchemaFieldInfo fieldInfo = result.getEditableSchemaFieldInfo().get(0);
    assertEquals(fieldInfo.getFieldPath(), "field2");
    assertNotNull(fieldInfo.getGlossaryTerms());
    assertEquals(fieldInfo.getGlossaryTerms().getTerms().size(), 1);
    assertEquals(
        fieldInfo.getGlossaryTerms().getTerms().get(0).getUrn().toString(),
        "urn:li:glossaryTerm:term1");
  }

  @Test
  public void testAddSecondTagDoesNotClobberExistingFieldPath() throws Exception {
    // The re-injection guard (get(keyField) == null) must not overwrite a fieldPath that is
    // already present on a round-tripped element.
    EditableSchemaMetadata initial = TEMPLATE.getDefault();

    JsonPatchBuilder first = Json.createPatchBuilder();
    first.add(
        "/editableSchemaFieldInfo/field1/globalTags/tags/urn:li:tag:tag1",
        Json.createObjectBuilder().add("tag", "urn:li:tag:tag1").build());
    EditableSchemaMetadata afterFirst = TEMPLATE.applyPatch(initial, first.build());

    JsonPatchBuilder second = Json.createPatchBuilder();
    second.add(
        "/editableSchemaFieldInfo/field1/globalTags/tags/urn:li:tag:tag2",
        Json.createObjectBuilder().add("tag", "urn:li:tag:tag2").build());
    EditableSchemaMetadata result = TEMPLATE.applyPatch(afterFirst, second.build());

    assertNotNull(result.getEditableSchemaFieldInfo());
    assertEquals(result.getEditableSchemaFieldInfo().size(), 1);
    EditableSchemaFieldInfo fieldInfo = result.getEditableSchemaFieldInfo().get(0);
    assertEquals(fieldInfo.getFieldPath(), "field1");
    assertEquals(fieldInfo.getGlobalTags().getTags().size(), 2);
  }
}
