package com.linkedin.datahub.graphql.types.form;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FieldFormPromptAssociationArray;
import com.linkedin.common.FormPromptAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FieldFormPromptAssociation;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.FormAssociation;
import com.linkedin.datahub.graphql.generated.FormPromptAssociation;
import com.linkedin.datahub.graphql.generated.FormPromptFieldAssociations;
import com.linkedin.datahub.graphql.generated.FormVerificationAssociation;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public class FormsMapper {

  public static final FormsMapper INSTANCE = new FormsMapper();

  public static com.linkedin.datahub.graphql.generated.Forms map(
      @Nonnull final Forms forms, @Nonnull final String entityUrn) {
    return INSTANCE.apply(forms, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.Forms apply(
      @Nonnull final Forms forms, @Nonnull final String entityUrn) {
    final List<FormAssociation> incompleteForms = new ArrayList<>();
    forms
        .getIncompleteForms()
        .forEach(
            formAssociation ->
                incompleteForms.add(this.mapFormAssociation(formAssociation, entityUrn)));
    final List<FormAssociation> completeForms = new ArrayList<>();
    forms
        .getCompletedForms()
        .forEach(
            formAssociation ->
                completeForms.add(this.mapFormAssociation(formAssociation, entityUrn)));
    final List<FormVerificationAssociation> verifications = new ArrayList<>();
    forms
        .getVerifications()
        .forEach(
            verificationAssociation ->
                verifications.add(this.mapVerificationAssociation(verificationAssociation)));

    return new com.linkedin.datahub.graphql.generated.Forms(
        incompleteForms, completeForms, verifications);
  }

  private FormAssociation mapFormAssociation(
      @Nonnull final com.linkedin.common.FormAssociation association,
      @Nonnull final String entityUrn) {
    FormAssociation result = new FormAssociation();
    result.setForm(
        Form.builder().setType(EntityType.FORM).setUrn(association.getUrn().toString()).build());
    result.setAssociatedUrn(entityUrn);
    result.setCompletedPrompts(this.mapPrompts(association.getCompletedPrompts()));
    result.setIncompletePrompts(this.mapPrompts(association.getIncompletePrompts()));
    return result;
  }

  private FormVerificationAssociation mapVerificationAssociation(
      @Nonnull final com.linkedin.common.FormVerificationAssociation verificationAssociation) {
    FormVerificationAssociation result = new FormVerificationAssociation();
    result.setForm(
        Form.builder()
            .setType(EntityType.FORM)
            .setUrn(verificationAssociation.getForm().toString())
            .build());
    if (verificationAssociation.hasLastModified()) {
      result.setLastModified(createAuditStamp(verificationAssociation.getLastModified()));
    }
    return result;
  }

  private List<FormPromptAssociation> mapPrompts(
      @Nonnull final FormPromptAssociationArray promptAssociations) {
    List<FormPromptAssociation> result = new ArrayList<>();
    promptAssociations.forEach(
        promptAssociation -> {
          FormPromptAssociation association = new FormPromptAssociation();
          association.setId(promptAssociation.getId());
          association.setLastModified(createAuditStamp(promptAssociation.getLastModified()));
          if (promptAssociation.hasFieldAssociations()) {
            association.setFieldAssociations(
                mapFieldAssociations(promptAssociation.getFieldAssociations()));
          }
          result.add(association);
        });
    return result;
  }

  private List<FieldFormPromptAssociation> mapFieldPrompts(
      @Nonnull final FieldFormPromptAssociationArray fieldPromptAssociations) {
    List<FieldFormPromptAssociation> result = new ArrayList<>();
    fieldPromptAssociations.forEach(
        fieldFormPromptAssociation -> {
          FieldFormPromptAssociation association = new FieldFormPromptAssociation();
          association.setFieldPath(fieldFormPromptAssociation.getFieldPath());
          association.setLastModified(
              createAuditStamp(fieldFormPromptAssociation.getLastModified()));
          result.add(association);
        });
    return result;
  }

  private FormPromptFieldAssociations mapFieldAssociations(
      com.linkedin.common.FormPromptFieldAssociations associationsObj) {
    final FormPromptFieldAssociations fieldAssociations = new FormPromptFieldAssociations();
    if (associationsObj.hasCompletedFieldPrompts()) {
      fieldAssociations.setCompletedFieldPrompts(
          this.mapFieldPrompts(associationsObj.getCompletedFieldPrompts()));
    }
    if (associationsObj.hasIncompleteFieldPrompts()) {
      fieldAssociations.setIncompleteFieldPrompts(
          this.mapFieldPrompts(associationsObj.getIncompleteFieldPrompts()));
    }
    return fieldAssociations;
  }

  private ResolvedAuditStamp createAuditStamp(AuditStamp auditStamp) {
    final ResolvedAuditStamp resolvedAuditStamp = new ResolvedAuditStamp();
    final CorpUser emptyCreatedUser = new CorpUser();
    emptyCreatedUser.setUrn(auditStamp.getActor().toString());
    resolvedAuditStamp.setActor(emptyCreatedUser);
    resolvedAuditStamp.setTime(auditStamp.getTime());
    return resolvedAuditStamp;
  }
}
