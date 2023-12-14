package datahub.client.patch.dataset;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class EditableSchemaMetadataPatchBuilder
    extends AbstractMultiFieldPatchBuilder<EditableSchemaMetadataPatchBuilder> {

  private static final String BASE_PATH = "/editableSchemaFieldInfo/";
  private static final String TAGS_PATH_EXTENSION = "/globalTags/tags/";
  private static final String TERMS_PATH_EXTENSION = "/glossaryTerms/terms/";
  private static final String TAG_KEY = "tag";
  private static final String URN_KEY = "urn";
  private static final String CONTEXT_KEY = "context";

  public EditableSchemaMetadataPatchBuilder addTag(
      @Nonnull TagAssociation tag, @Nonnull String fieldPath) {
    ObjectNode value = instance.objectNode();
    value.put(TAG_KEY, tag.getTag().toString());
    if (tag.getContext() != null) {
      value.put(CONTEXT_KEY, tag.getContext());
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + fieldPath + TAGS_PATH_EXTENSION + tag.getTag(),
            value));
    return this;
  }

  public EditableSchemaMetadataPatchBuilder removeTag(
      @Nonnull TagUrn tag, @Nonnull String fieldPath) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH + fieldPath + TAGS_PATH_EXTENSION + tag,
            null));
    return this;
  }

  public EditableSchemaMetadataPatchBuilder addGlossaryTerm(
      @Nonnull GlossaryTermAssociation term, @Nonnull String fieldPath) {
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, term.getUrn().toString());
    if (term.getContext() != null) {
      value.put(CONTEXT_KEY, term.getContext());
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + fieldPath + TERMS_PATH_EXTENSION + term.getUrn(),
            value));
    return this;
  }

  public EditableSchemaMetadataPatchBuilder removeGlossaryTerm(
      @Nonnull GlossaryTermUrn term, @Nonnull String fieldPath) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH + fieldPath + TERMS_PATH_EXTENSION + term,
            null));
    return this;
  }

  @Override
  protected String getAspectName() {
    return EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATASET_ENTITY_NAME;
  }
}
