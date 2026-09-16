package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class EditableDatasetPropertiesPatchBuilder
    extends AbstractMultiFieldPatchBuilder<EditableDatasetPropertiesPatchBuilder> {

  public static final String BASE_PATH = "/";

  public static final String DESCRIPTION_KEY = "description";
  public static final String NAME_KEY = "name";
  public static final String LAST_MODIFIED_KEY = "lastModified";

  public EditableDatasetPropertiesPatchBuilder setName(@Nullable String name) {
    if (name == null) {
      this.pathValues.add(
          ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + NAME_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(), BASE_PATH + NAME_KEY, instance.textNode(name)));
    }
    return this;
  }

  public EditableDatasetPropertiesPatchBuilder setDescription(@Nullable String description) {
    if (description == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + DESCRIPTION_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + DESCRIPTION_KEY,
              instance.textNode(description)));
    }
    return this;
  }

  @Override
  protected String getAspectName() {
    return EDITABLE_DATASET_PROPERTIES_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATASET_ENTITY_NAME;
  }
}
