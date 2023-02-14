package datahub.client.patch.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.TagAssociation;
import datahub.client.patch.AbstractPatchBuilder;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


/**
 * Update a single tag or term
 */
public class EditableSchemaMetadataPatchBuilder extends AbstractPatchBuilder<EditableSchemaMetadataPatchBuilder> {

  private static final String BASE_PATH = "/editableSchemaFieldInfo/";
  private static final String TAG_KEY = "tag";
  private static final String URN_KEY = "urn";
  private static final String CONTEXT_KEY = "context";

  private final String fieldPath;
  private TagAssociation tag = null;
  private GlossaryTermAssociation glossaryTerm = null;

  public EditableSchemaMetadataPatchBuilder(String fieldPath) {
    this.fieldPath = fieldPath;
  }

  public EditableSchemaMetadataPatchBuilder tag(TagAssociation tag) {
    this.tag = tag;
    return this;
  }

  public EditableSchemaMetadataPatchBuilder glossaryTerm(GlossaryTermAssociation glossaryTerm) {
    this.glossaryTerm = glossaryTerm;
    return this;
  }

  @Override
  protected Stream<Object> getRequiredProperties() {
    Boolean eitherTagOrTerm = tag == null || glossaryTerm == null ? true : null;

    return Stream.of(this.op, this.targetEntityUrn, this.fieldPath, eitherTagOrTerm);
  }

  @Override
  protected String getPath() {
    String extendedKey = "";

    if (tag != null) {
      extendedKey = "/globalTags/tags/" + tag.getTag();
    } else if (glossaryTerm != null) {
      extendedKey = "/glossaryTerms/terms/" + glossaryTerm.getUrn();
    }

    return BASE_PATH + fieldPath + extendedKey;
  }

  @Override
  protected JsonNode getValue() {
    ObjectNode value = instance.objectNode();

    if (this.tag != null) {
      value.put(TAG_KEY, tag.getTag().toString());
      if (tag.getContext() != null) {
        value.put(CONTEXT_KEY, tag.getContext());
      }
    } else if (this.glossaryTerm != null) {
      value.put(URN_KEY, glossaryTerm.getUrn().toString());
      if (glossaryTerm.getContext() != null) {
        value.put(CONTEXT_KEY, glossaryTerm.getContext());
      }
    }

    return value;
  }

  @Override
  protected String getAspectName() {
    return EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
  }
}
