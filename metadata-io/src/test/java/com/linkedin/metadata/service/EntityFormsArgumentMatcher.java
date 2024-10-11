package com.linkedin.metadata.service;

import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.FormVerificationAssociation;
import com.linkedin.common.Forms;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.List;
import org.mockito.ArgumentMatcher;

public class EntityFormsArgumentMatcher implements ArgumentMatcher<List<MetadataChangeProposal>> {

  private List<MetadataChangeProposal> leftList;

  public EntityFormsArgumentMatcher(List<MetadataChangeProposal> leftList) {
    this.leftList = leftList;
  }

  @Override
  public boolean matches(List<MetadataChangeProposal> rightList) {
    return rightList.stream().allMatch(right ->
        leftList.stream().anyMatch(left ->
            left.getEntityType().equals(right.getEntityType())
                && left.getAspectName().equals(right.getAspectName())
                && left.getChangeType().equals(right.getChangeType())
                && formsMatches(left.getAspect(), right.getAspect())));
  }

  private boolean formsMatches(GenericAspect left, GenericAspect right) {
    Forms leftProps =
        GenericRecordUtils.deserializeAspect(left.getValue(), "application/json", Forms.class);

    Forms rightProps =
        GenericRecordUtils.deserializeAspect(right.getValue(), "application/json", Forms.class);

    if (leftProps.hasCompletedForms()) {
      boolean res =
          formAssociationsMatch(leftProps.getCompletedForms(), rightProps.getCompletedForms());
      if (!res) {
        return false;
      }
    }

    if (rightProps.hasIncompleteForms()) {
      boolean res =
          formAssociationsMatch(leftProps.getIncompleteForms(), rightProps.getIncompleteForms());
      if (!res) {
        return false;
      }
    }

    if (rightProps.hasVerifications()) {
      boolean res = verificationsMatch(leftProps.getVerifications(), rightProps.getVerifications());
      if (!res) {
        return false;
      }
    }

    // Verify required fields.
    return true;
  }

  private boolean formAssociationsMatch(List<FormAssociation> left, List<FormAssociation> right) {
    for (int i = 0; i < left.size(); i++) {
      FormAssociation leftAssociation = left.get(i);
      FormAssociation rightAssociation = right.get(i);
      boolean res = formAssociationMatch(leftAssociation, rightAssociation);
      if (!res) {
        return false;
      }
    }
    // Verify required fields.
    return true;
  }

  private boolean formAssociationMatch(FormAssociation left, FormAssociation right) {
    return left.getUrn().equals(right.getUrn())
        && formPromptsMatch(left.getIncompletePrompts(), right.getIncompletePrompts())
        && formPromptsMatch(left.getCompletedPrompts(), right.getCompletedPrompts());
  }

  private boolean formPromptsMatch(
      List<FormPromptAssociation> left, List<FormPromptAssociation> right) {
    for (int i = 0; i < left.size(); i++) {
      FormPromptAssociation leftPrompt = left.get(i);
      FormPromptAssociation rightPrompt = right.get(i);
      boolean res = formPromptMatch(leftPrompt, rightPrompt);
      if (!res) {
        return false;
      }
    }
    // Verify required fields.
    return true;
  }

  private boolean formPromptMatch(FormPromptAssociation left, FormPromptAssociation right) {
    return left.getId().equals(right.getId());
  }

  private boolean verificationsMatch(
      List<FormVerificationAssociation> left, List<FormVerificationAssociation> right) {
    for (int i = 0; i < left.size(); i++) {
      FormVerificationAssociation leftVerification = left.get(i);
      FormVerificationAssociation rightVerification = right.get(i);
      boolean res = verificationMatch(leftVerification, rightVerification);
      if (!res) {
        return false;
      }
    }
    // Verify required fields.
    return true;
  }

  private boolean verificationMatch(
      FormVerificationAssociation left, FormVerificationAssociation right) {
    return left.getForm().equals(right.getForm());
  }
}
