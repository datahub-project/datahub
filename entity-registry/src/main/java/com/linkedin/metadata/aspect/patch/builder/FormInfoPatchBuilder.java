package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormType;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class FormInfoPatchBuilder extends AbstractMultiFieldPatchBuilder<FormInfoPatchBuilder> {

  public static final String PATH_DELIM = "/";
  public static final String NAME_FIELD = "name";
  public static final String DESCRIPTION_FIELD = "description";
  public static final String TYPE_FIELD = "type";
  public static final String PROMPTS_FIELD = "prompts";
  public static final String ACTORS_FIELD = "actors";
  public static final String OWNERS_FIELD = "owners";
  public static final String USERS_FIELD = "users";
  public static final String GROUPS_FIELD = "groups";

  public FormInfoPatchBuilder setName(@Nonnull String name) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), PATH_DELIM + NAME_FIELD, instance.textNode(name)));
    return this;
  }

  public FormInfoPatchBuilder setDescription(@Nullable String description) {
    if (description == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), PATH_DELIM + DESCRIPTION_FIELD, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              PATH_DELIM + DESCRIPTION_FIELD,
              instance.textNode(description)));
    }
    return this;
  }

  public FormInfoPatchBuilder setType(@Nonnull FormType formType) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + TYPE_FIELD,
            instance.textNode(formType.toString())));
    return this;
  }

  public FormInfoPatchBuilder addPrompt(@Nonnull FormPrompt prompt) {
    try {
      ObjectNode promptNode =
          (ObjectNode) new ObjectMapper().readTree(RecordUtils.toJsonString(prompt));
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              PATH_DELIM + PROMPTS_FIELD + PATH_DELIM + prompt.getId(),
              promptNode));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to add prompt, failed to parse provided aspect json.", e);
    }
  }

  public FormInfoPatchBuilder addPrompts(@Nonnull List<FormPrompt> prompts) {
    try {
      prompts.forEach(this::addPrompt);
      return this;
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to add prompts.", e);
    }
  }

  public FormInfoPatchBuilder removePrompt(@Nonnull String promptId) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            PATH_DELIM + PROMPTS_FIELD + PATH_DELIM + promptId,
            null));
    return this;
  }

  public FormInfoPatchBuilder removePrompts(@Nonnull List<String> promptIds) {
    promptIds.forEach(this::removePrompt);
    return this;
  }

  public FormInfoPatchBuilder setOwnershipForm(boolean isOwnershipForm) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + ACTORS_FIELD + PATH_DELIM + OWNERS_FIELD,
            instance.booleanNode(isOwnershipForm)));
    return this;
  }

  public FormInfoPatchBuilder addAssignedUser(@Nonnull String userUrn) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + ACTORS_FIELD + PATH_DELIM + USERS_FIELD + PATH_DELIM + userUrn,
            instance.textNode(userUrn)));
    return this;
  }

  public FormInfoPatchBuilder removeAssignedUser(@Nonnull String userUrn) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            PATH_DELIM + ACTORS_FIELD + PATH_DELIM + USERS_FIELD + PATH_DELIM + userUrn,
            instance.textNode(userUrn)));
    return this;
  }

  public FormInfoPatchBuilder addAssignedGroup(@Nonnull String groupUrn) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + ACTORS_FIELD + PATH_DELIM + GROUPS_FIELD + PATH_DELIM + groupUrn,
            instance.textNode(groupUrn)));
    return this;
  }

  public FormInfoPatchBuilder removeAssignedGroup(@Nonnull String groupUrn) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            PATH_DELIM + ACTORS_FIELD + PATH_DELIM + GROUPS_FIELD + PATH_DELIM + groupUrn,
            instance.textNode(groupUrn)));
    return this;
  }

  @Override
  protected String getAspectName() {
    return FORM_INFO_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return FORM_ENTITY_NAME;
  }
}
