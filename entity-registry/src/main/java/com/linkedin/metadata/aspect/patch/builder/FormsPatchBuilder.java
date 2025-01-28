package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.FormVerificationAssociation;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.template.form.FormAssociationTemplate;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class FormsPatchBuilder extends AbstractMultiFieldPatchBuilder<FormsPatchBuilder> {

  public static final String FORMS_FIELD = "forms";
  public static final String PROMPTS_FIELD = "prompts";
  private static final String IS_COMPLETE_FIELD = "isComplete";
  private static final String VERIFICATIONS_FIELD = "verifications";

  public FormsPatchBuilder completePrompt(
      @Nonnull Urn formUrn, @Nonnull FormPromptAssociation promptAssociation) {
    try {
      ObjectNode promptNode =
          (ObjectNode) new ObjectMapper().readTree(RecordUtils.toJsonString(promptAssociation));
      promptNode.set(IS_COMPLETE_FIELD, instance.booleanNode(true));
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              "/"
                  + FORMS_FIELD
                  + "/"
                  + formUrn
                  + "/"
                  + PROMPTS_FIELD
                  + "/"
                  + promptAssociation.getId(),
              promptNode));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to mark form prompt as complete, failed to parse provided aspect json.", e);
    }
  }

  public FormsPatchBuilder markPromptIncomplete(
      @Nonnull Urn formUrn, @Nonnull FormPromptAssociation promptAssociation) {
    try {
      ObjectNode promptNode =
          (ObjectNode) new ObjectMapper().readTree(RecordUtils.toJsonString(promptAssociation));
      promptNode.set(IS_COMPLETE_FIELD, instance.booleanNode(false));
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              "/"
                  + FORMS_FIELD
                  + "/"
                  + formUrn
                  + "/"
                  + PROMPTS_FIELD
                  + "/"
                  + promptAssociation.getId(),
              promptNode));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to mark form prompt as complete, failed to parse provided aspect json.", e);
    }
  }

  public FormsPatchBuilder completeForm(@Nonnull FormAssociation formAssociation) {
    try {
      ObjectNode formNode =
          (ObjectNode) new ObjectMapper().readTree(RecordUtils.toJsonString(formAssociation));
      formNode.set(IS_COMPLETE_FIELD, instance.booleanNode(true));
      JsonNode promptsMap = FormAssociationTemplate.createPromptsMap(formNode);
      formNode.set(PROMPTS_FIELD, promptsMap);
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              "/" + FORMS_FIELD + "/" + formAssociation.getUrn(),
              formNode));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to mark form as complete, failed to parse provided aspect json.", e);
    }
  }

  public FormsPatchBuilder setFormIncomplete(@Nonnull FormAssociation formAssociation) {
    try {
      ObjectNode formNode =
          (ObjectNode) new ObjectMapper().readTree(RecordUtils.toJsonString(formAssociation));
      formNode.set(IS_COMPLETE_FIELD, instance.booleanNode(false));
      JsonNode promptsMap = FormAssociationTemplate.createPromptsMap(formNode);
      formNode.set(PROMPTS_FIELD, promptsMap);
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              "/" + FORMS_FIELD + "/" + encodeValueUrn(formAssociation.getUrn()),
              formNode));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to mark form as incomplete, failed to parse provided aspect json.", e);
    }
  }

  public FormsPatchBuilder verifyForm(
      @Nonnull FormVerificationAssociation verificationAssociation) {
    try {
      ObjectNode formNode =
          (ObjectNode)
              new ObjectMapper().readTree(RecordUtils.toJsonString(verificationAssociation));
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              "/" + VERIFICATIONS_FIELD + "/" + encodeValueUrn(verificationAssociation.getForm()),
              formNode));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to verify form, failed to parse provided aspect json.", e);
    }
  }

  public FormsPatchBuilder removeForm(Urn formUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            "/" + FORMS_FIELD + "/" + encodeValueUrn(formUrn),
            null));
    return this;
  }

  public FormsPatchBuilder removeFormVerification(Urn formUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            "/" + VERIFICATIONS_FIELD + "/" + encodeValueUrn(formUrn),
            null));
    return this;
  }

  @Override
  protected String getAspectName() {
    return FORMS_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    if (this.targetEntityUrn == null) {
      throw new IllegalStateException(
          "Target Entity Urn must be set to determine entity type before building Patch.");
    }
    return this.targetEntityUrn.getEntityType();
  }
}
