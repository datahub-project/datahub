package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DomainPropertiesPatchBuilder
    extends AbstractMultiFieldPatchBuilder<DomainPropertiesPatchBuilder> {
  public static final String BASE_PATH = "/";
  public static final String DESCRIPTION_KEY = "description";
  public static final String LAST_MODIFIED_KEY = "lastModified";

  public DomainPropertiesPatchBuilder setDescription(@Nullable String description) {
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
    return DOMAIN_PROPERTIES_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DOMAIN_ENTITY_NAME;
  }
}
