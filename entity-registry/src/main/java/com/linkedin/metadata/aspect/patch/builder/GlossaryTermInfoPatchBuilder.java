package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class GlossaryTermInfoPatchBuilder
    extends AbstractMultiFieldPatchBuilder<GlossaryTermInfoPatchBuilder> {
  public static final String BASE_PATH = "/";
  public static final String DEFINITION_KEY = "definition";

  public GlossaryTermInfoPatchBuilder setDescription(@Nullable String description) {
    if (description == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + DEFINITION_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + DEFINITION_KEY,
              instance.textNode(description)));
    }
    return this;
  }

  @Override
  protected String getAspectName() {
    return GLOSSARY_TERM_INFO_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return GLOSSARY_TERM_ENTITY_NAME;
  }
}
