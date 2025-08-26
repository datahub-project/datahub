package com.linkedin.metadata.aspect.patch.template.form;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.CompoundKeyTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

/** Combines fine grained lineage array into a map using upstream and downstream types as keys, */
public class FormsTemplate extends CompoundKeyTemplate<Forms> {

  private static final String INCOMPLETE_FORMS_FIELD_NAME = "incompleteForms";
  private static final String COMPLETED_FORMS_FIELD_NAME = "completedForms";
  private static final String VERIFICATIONS_FIELD_NAME = "verifications";
  private static final String FORMS_FIELD_NAME = "forms";
  private static final String URN_FIELD_NAME = "urn";
  private static final String FORM_FIELD_NAME = "form";
  private static final String IS_COMPLETE_FIELD_NAME = "isComplete";

  @Override
  public Forms getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof Forms) {
      return (Forms) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to Forms");
  }

  @Override
  public Class<Forms> getTemplateType() {
    return Forms.class;
  }

  @Nonnull
  @Override
  public Forms getDefault() {
    return new Forms()
        .setIncompleteForms(new FormAssociationArray())
        .setCompletedForms(new FormAssociationArray());
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode = baseNode;

    FormAssociationTemplate formAssociationTemplate = new FormAssociationTemplate();

    // Combine incomplete/completed forms into one forms list with an isComplete flag attached to
    // each.
    // This is necessary since we may not know the current state of the form when patching.
    JsonNode formsMap = instance.objectNode();
    if (transformedNode.get(INCOMPLETE_FORMS_FIELD_NAME) != null) {
      transformedNode =
          arrayFieldToMap(
              transformedNode,
              INCOMPLETE_FORMS_FIELD_NAME,
              Collections.singletonList(URN_FIELD_NAME));
      transformedNode
          .get(INCOMPLETE_FORMS_FIELD_NAME)
          .elements()
          .forEachRemaining(
              node -> {
                JsonNode urn = node.get(URN_FIELD_NAME);
                ((ObjectNode) node).set(IS_COMPLETE_FIELD_NAME, instance.booleanNode(false));
                ((ObjectNode) formsMap)
                    .set(urn.asText(), formAssociationTemplate.transformFields(node));
              });
    }

    if (transformedNode.get(COMPLETED_FORMS_FIELD_NAME) != null) {
      transformedNode =
          arrayFieldToMap(
              transformedNode,
              COMPLETED_FORMS_FIELD_NAME,
              Collections.singletonList(URN_FIELD_NAME));
      transformedNode
          .get(COMPLETED_FORMS_FIELD_NAME)
          .elements()
          .forEachRemaining(
              node -> {
                JsonNode urn = node.get(URN_FIELD_NAME);
                ((ObjectNode) node).set(IS_COMPLETE_FIELD_NAME, instance.booleanNode(true));
                ((ObjectNode) formsMap)
                    .set(urn.asText(), formAssociationTemplate.transformFields(node));
              });
    }

    if (transformedNode.get(VERIFICATIONS_FIELD_NAME) != null) {
      transformedNode =
          arrayFieldToMap(
              transformedNode,
              VERIFICATIONS_FIELD_NAME,
              Collections.singletonList(FORM_FIELD_NAME));
    }

    ((ObjectNode) transformedNode).set(FORMS_FIELD_NAME, formsMap);

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(patched, FORMS_FIELD_NAME, Collections.singletonList(URN_FIELD_NAME));

    FormAssociationTemplate formAssociationTemplate = new FormAssociationTemplate();

    // Breaks the combined forms field into incomplete/completed forms in the main aspect
    ArrayNode incompleteForms = instance.arrayNode();
    ArrayNode completedForms = instance.arrayNode();
    rebasedNode
        .get(FORMS_FIELD_NAME)
        .elements()
        .forEachRemaining(
            node -> {
              boolean isComplete = node.get(IS_COMPLETE_FIELD_NAME).asBoolean();
              JsonNode rebased = formAssociationTemplate.rebaseFields(node);
              if (isComplete) {
                completedForms.add(rebased);
              } else {
                incompleteForms.add(rebased);
              }
            });
    if (rebasedNode.get(VERIFICATIONS_FIELD_NAME) != null) {
      rebasedNode =
          transformedMapToArray(
              rebasedNode, VERIFICATIONS_FIELD_NAME, Collections.singletonList(FORM_FIELD_NAME));
    }
    ((ObjectNode) rebasedNode).set(INCOMPLETE_FORMS_FIELD_NAME, incompleteForms);
    ((ObjectNode) rebasedNode).set(COMPLETED_FORMS_FIELD_NAME, completedForms);
    ((ObjectNode) rebasedNode).remove(FORMS_FIELD_NAME);

    return rebasedNode;
  }
}
