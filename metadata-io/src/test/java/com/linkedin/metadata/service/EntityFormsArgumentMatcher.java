package com.linkedin.metadata.service;

import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.FormVerificationAssociation;
import com.linkedin.common.Forms;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.ArgumentMatcher;

public class EntityFormsArgumentMatcher implements ArgumentMatcher<List<MetadataChangeProposal>> {

  private List<MetadataChangeProposal> leftList;
  private OperationContext opContext;
  private Forms existingForms;

  public EntityFormsArgumentMatcher(
      List<MetadataChangeProposal> leftList, OperationContext opContext, Forms existingForms) {
    this.leftList = leftList;
    this.opContext = opContext;
    this.existingForms = existingForms;
  }

  @Override
  public boolean matches(List<MetadataChangeProposal> rightList) {
    List<MetadataChangeProposal> convertedList =
        rightList.stream()
            .filter(mcp -> mcp.getChangeType().equals(ChangeType.PATCH))
            .map(
                mcp ->
                    PatchItemImpl.builder()
                        .build(mcp, opContext.getAuditStamp(), opContext.getEntityRegistry())
                        .applyPatch(existingForms, opContext.getAspectRetriever()))
            .map(this::getPatchedMcp)
            .collect(Collectors.toList());
    return convertedList.stream()
        .allMatch(
            right ->
                leftList.stream()
                    .anyMatch(
                        left ->
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

  private MetadataChangeProposal getPatchedMcp(ChangeItemImpl changeItem) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(changeItem.getUrn());
    mcp.setChangeType(changeItem.getChangeType());
    mcp.setEntityType(changeItem.getEntitySpec().getName());
    mcp.setAspectName(changeItem.getAspectName());
    mcp.setAspect(GenericRecordUtils.serializeAspect(changeItem.getRecordTemplate()));
    mcp.setSystemMetadata(changeItem.getSystemMetadata());
    mcp.setEntityKeyAspect(
        GenericRecordUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(
                changeItem.getUrn(), changeItem.getEntitySpec().getKeyAspectSpec())));
    if (!changeItem.getHeaders().isEmpty()) {
      mcp.setHeaders(new StringMap(changeItem.getHeaders()));
    }
    return mcp;
  }
}
