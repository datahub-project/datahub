package com.linkedin.metadata.aspect.patch.template.form;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormPromptAssociationArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.CompoundKeyTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class FormAssociationTemplate extends CompoundKeyTemplate<FormAssociation> {

  private static final String INCOMPLETE_PROMPTS_FIELD_NAME = "incompletePrompts";
  private static final String COMPLETED_PROMPTS_FIELD_NAME = "completedPrompts";
  private static final String PROMPTS_FIELD_NAME = "prompts";
  private static final String ID_FIELD_NAME = "id";
  private static final String IS_COMPLETE_FIELD_NAME = "isComplete";

  @Override
  public FormAssociation getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof FormAssociation) {
      return (FormAssociation) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to FormAssociation");
  }

  @Override
  public Class<FormAssociation> getTemplateType() {
    return FormAssociation.class;
  }

  @Nonnull
  @Override
  public FormAssociation getDefault() {
    return new FormAssociation()
        .setIncompletePrompts(new FormPromptAssociationArray())
        .setCompletedPrompts(new FormPromptAssociationArray());
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode = baseNode;
    // first transform the two fields
    if (transformedNode.get(INCOMPLETE_PROMPTS_FIELD_NAME) != null) {
      transformedNode =
          arrayFieldToMap(
              transformedNode,
              INCOMPLETE_PROMPTS_FIELD_NAME,
              Collections.singletonList(ID_FIELD_NAME));
    }

    if (transformedNode.get(COMPLETED_PROMPTS_FIELD_NAME) != null) {
      transformedNode =
          arrayFieldToMap(
              transformedNode,
              COMPLETED_PROMPTS_FIELD_NAME,
              Collections.singletonList(ID_FIELD_NAME));
    }

    // Combine incomplete/completed prompts into one prompts list with an isComplete flag attached
    // to each. This is necessary for patching when we don't know which list the item is in.
    JsonNode promptsMap = createPromptsMap(transformedNode);

    ((ObjectNode) transformedNode).set(PROMPTS_FIELD_NAME, promptsMap);

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched, PROMPTS_FIELD_NAME, Collections.singletonList(ID_FIELD_NAME));

    // Breaks the combined prompts field into incomplete/completed prompts in the main aspect
    ArrayNode incompletePrompts = instance.arrayNode();
    ArrayNode completedPrompts = instance.arrayNode();
    rebasedNode
        .get(PROMPTS_FIELD_NAME)
        .elements()
        .forEachRemaining(
            node -> {
              boolean isComplete = node.get(IS_COMPLETE_FIELD_NAME).asBoolean();
              ((ObjectNode) node).remove(IS_COMPLETE_FIELD_NAME);
              if (isComplete) {
                completedPrompts.add(node);
              } else {
                incompletePrompts.add(node);
              }
            });
    ((ObjectNode) rebasedNode).set(INCOMPLETE_PROMPTS_FIELD_NAME, incompletePrompts);
    ((ObjectNode) rebasedNode).set(COMPLETED_PROMPTS_FIELD_NAME, completedPrompts);
    ((ObjectNode) rebasedNode).remove(PROMPTS_FIELD_NAME);
    ((ObjectNode) rebasedNode).remove(IS_COMPLETE_FIELD_NAME);

    return rebasedNode;
  }

  /*
   * Combine incomplete/completed prompts into one prompts list with an isComplete flag attached to each.
   * This is necessary since we may not know the current state of the form when patching.
   */
  public static JsonNode createPromptsMap(JsonNode formAssociationNode) {
    JsonNode promptsMap = instance.objectNode();
    if (formAssociationNode.get(INCOMPLETE_PROMPTS_FIELD_NAME) != null) {
      formAssociationNode
          .get(INCOMPLETE_PROMPTS_FIELD_NAME)
          .elements()
          .forEachRemaining(
              node -> {
                ((ObjectNode) node).set(IS_COMPLETE_FIELD_NAME, instance.booleanNode(false));
                JsonNode id = node.get(ID_FIELD_NAME);
                ((ObjectNode) promptsMap).set(id.asText(), node);
              });
    }
    if (formAssociationNode.get(COMPLETED_PROMPTS_FIELD_NAME) != null) {
      formAssociationNode
          .get(COMPLETED_PROMPTS_FIELD_NAME)
          .elements()
          .forEachRemaining(
              node -> {
                ((ObjectNode) node).set(IS_COMPLETE_FIELD_NAME, instance.booleanNode(true));
                JsonNode id = node.get(ID_FIELD_NAME);
                ((ObjectNode) promptsMap).set(id.asText(), node);
              });
    }
    return promptsMap;
  }
}
