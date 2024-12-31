package com.linkedin.metadata.aspect.patch.template.form;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.aspect.patch.template.CompoundKeyTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class FormInfoTemplate extends CompoundKeyTemplate<FormInfo> {

  private static final String PROMPTS_FIELD_NAME = "prompts";
  private static final String PROMPT_ID_FIELD_NAME = "id";
  private static final String ACTORS_FIELD_NAME = "actors";
  private static final String USERS_FIELD_NAME = "users";
  private static final String GROUPS_FIELD_NAME = "groups";

  @Override
  public FormInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof FormInfo) {
      return (FormInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to FormInfo");
  }

  @Override
  public Class<FormInfo> getTemplateType() {
    return FormInfo.class;
  }

  @Nonnull
  @Override
  public FormInfo getDefault() {
    FormInfo formInfo = new FormInfo();
    formInfo.setName("");

    return formInfo;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode, PROMPTS_FIELD_NAME, Collections.singletonList(PROMPT_ID_FIELD_NAME));

    JsonNode actors = transformedNode.get(ACTORS_FIELD_NAME);
    if (actors == null) {
      actors = instance.objectNode();
    }

    JsonNode transformedActorsNode =
        arrayFieldToMap(actors, USERS_FIELD_NAME, Collections.emptyList());
    transformedActorsNode =
        arrayFieldToMap(transformedActorsNode, GROUPS_FIELD_NAME, Collections.emptyList());
    ((ObjectNode) transformedNode).set(ACTORS_FIELD_NAME, transformedActorsNode);

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode transformedNode =
        transformedMapToArray(
            patched, PROMPTS_FIELD_NAME, Collections.singletonList(PROMPT_ID_FIELD_NAME));

    JsonNode actors = transformedNode.get(ACTORS_FIELD_NAME);
    if (actors == null) {
      actors = instance.objectNode();
    }

    JsonNode transformedActorsNode =
        transformedMapToArray(actors, USERS_FIELD_NAME, Collections.emptyList());
    transformedActorsNode =
        transformedMapToArray(transformedActorsNode, GROUPS_FIELD_NAME, Collections.emptyList());
    ((ObjectNode) transformedNode).set(ACTORS_FIELD_NAME, transformedActorsNode);

    return transformedNode;
  }
}
