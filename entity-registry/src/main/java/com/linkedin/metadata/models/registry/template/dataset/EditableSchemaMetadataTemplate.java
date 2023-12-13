package com.linkedin.metadata.models.registry.template.dataset;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.registry.template.CompoundKeyTemplate;
import com.linkedin.metadata.models.registry.template.common.GlobalTagsTemplate;
import com.linkedin.metadata.models.registry.template.common.GlossaryTermsTemplate;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.util.Collections;
import javax.annotation.Nonnull;

public class EditableSchemaMetadataTemplate extends CompoundKeyTemplate<EditableSchemaMetadata> {

  private static final String EDITABLE_SCHEMA_FIELD_INFO_FIELD_NAME = "editableSchemaFieldInfo";
  private static final String FIELDPATH_FIELD_NAME = "fieldPath";
  private static final String GLOBAL_TAGS_FIELD_NAME = "globalTags";
  private static final String GLOSSARY_TERMS_FIELD_NAME = "glossaryTerms";

  @Override
  public EditableSchemaMetadata getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableSchemaMetadata) {
      return (EditableSchemaMetadata) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableSchemaMetadata");
  }

  @Override
  public Class<EditableSchemaMetadata> getTemplateType() {
    return EditableSchemaMetadata.class;
  }

  @Nonnull
  @Override
  public EditableSchemaMetadata getDefault() {
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    return new EditableSchemaMetadata()
        .setCreated(auditStamp)
        .setLastModified(auditStamp)
        .setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray());
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode,
            EDITABLE_SCHEMA_FIELD_INFO_FIELD_NAME,
            Collections.singletonList(FIELDPATH_FIELD_NAME));
    // Create temporary templates for array subfields
    GlobalTagsTemplate globalTagsTemplate = new GlobalTagsTemplate();
    GlossaryTermsTemplate glossaryTermsTemplate = new GlossaryTermsTemplate();

    // Apply template transforms to array subfields
    transformedNode
        .get(EDITABLE_SCHEMA_FIELD_INFO_FIELD_NAME)
        .elements()
        .forEachRemaining(
            node -> {
              JsonNode globalTags = node.get(GLOBAL_TAGS_FIELD_NAME);
              JsonNode glossaryTerms = node.get(GLOSSARY_TERMS_FIELD_NAME);
              if (globalTags != null) {
                ((ObjectNode) node)
                    .set(
                        GLOBAL_TAGS_FIELD_NAME,
                        globalTagsTemplate.transformFields(node.get(GLOBAL_TAGS_FIELD_NAME)));
              }
              if (glossaryTerms != null) {
                ((ObjectNode) node)
                    .set(
                        GLOSSARY_TERMS_FIELD_NAME,
                        glossaryTermsTemplate.transformFields(node.get(GLOSSARY_TERMS_FIELD_NAME)));
              }
            });
    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched,
            EDITABLE_SCHEMA_FIELD_INFO_FIELD_NAME,
            Collections.singletonList(FIELDPATH_FIELD_NAME));
    // Create temporary templates for array subfields
    GlobalTagsTemplate globalTagsTemplate = new GlobalTagsTemplate();
    GlossaryTermsTemplate glossaryTermsTemplate = new GlossaryTermsTemplate();

    // Apply template rebases to array subfields
    rebasedNode
        .get(EDITABLE_SCHEMA_FIELD_INFO_FIELD_NAME)
        .elements()
        .forEachRemaining(
            node -> {
              JsonNode globalTags = node.get(GLOBAL_TAGS_FIELD_NAME);
              JsonNode glossaryTerms = node.get(GLOSSARY_TERMS_FIELD_NAME);
              if (globalTags != null) {
                ((ObjectNode) node)
                    .set(GLOBAL_TAGS_FIELD_NAME, globalTagsTemplate.rebaseFields(globalTags));
              }
              if (glossaryTerms != null) {
                ((ObjectNode) node)
                    .set(
                        GLOSSARY_TERMS_FIELD_NAME,
                        glossaryTermsTemplate.rebaseFields(glossaryTerms));
              }
            });

    return rebasedNode;
  }
}
