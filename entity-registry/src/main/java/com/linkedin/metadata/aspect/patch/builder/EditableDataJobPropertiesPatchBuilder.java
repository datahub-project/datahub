package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class EditableDataJobPropertiesPatchBuilder
    extends AbstractMultiFieldPatchBuilder<EditableDataJobPropertiesPatchBuilder> {
  public static final String BASE_PATH = "/";
  public static final String DESCRIPTION_KEY = "description";
  public static final String LAST_MODIFIED_KEY = "lastModified";

  public EditableDataJobPropertiesPatchBuilder setDescription(@Nullable String description) {
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

  public EditableDataJobPropertiesPatchBuilder setLastModified(@Nonnull AuditStamp auditStamp) {
    try {
      ObjectNode auditStampNode =
          (ObjectNode) new ObjectMapper().readTree(RecordUtils.toJsonString(auditStamp));
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(), BASE_PATH + LAST_MODIFIED_KEY, auditStampNode));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to set last modified, failed to parse provided aspect json.", e);
    }
  }

  @Override
  protected String getAspectName() {
    return EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATA_JOB_ENTITY_NAME;
  }
}
