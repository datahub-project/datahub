package com.linkedin.datahub.graphql.types.form;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FieldFormPromptAssociationArray;
import com.linkedin.common.FormPromptAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DocumentationResponse;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainPromptResponse;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FieldFormPromptAssociation;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.FormAssociation;
import com.linkedin.datahub.graphql.generated.FormPromptAssociation;
import com.linkedin.datahub.graphql.generated.FormPromptFieldAssociations;
import com.linkedin.datahub.graphql.generated.FormPromptResponse;
import com.linkedin.datahub.graphql.generated.FormVerificationAssociation;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.GlossaryTermsPromptResponse;
import com.linkedin.datahub.graphql.generated.NumberValue;
import com.linkedin.datahub.graphql.generated.OwnershipPromptResponse;
import com.linkedin.datahub.graphql.generated.PropertyValue;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.datahub.graphql.generated.StringValue;
import com.linkedin.datahub.graphql.generated.StructuredPropertyPromptResponse;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
          if (promptAssociation.getResponse() != null) {
            association.setResponse(mapPromptResponse(promptAssociation.getResponse()));
          }
          result.add(association);
        });
    return result;
  }

  private FormPromptResponse mapPromptResponse(
      @Nonnull final com.linkedin.common.FormPromptResponse promptResponse) {
    FormPromptResponse result = new FormPromptResponse();
    if (promptResponse.getStructuredPropertyResponse() != null) {
      com.linkedin.common.StructuredPropertyPromptResponse gmsPropertyResponse =
          promptResponse.getStructuredPropertyResponse();
      StructuredPropertyPromptResponse propertyResponse = new StructuredPropertyPromptResponse();
      propertyResponse.setPropertyUrn(gmsPropertyResponse.getPropertyUrn().toString());
      List<PropertyValue> values = new ArrayList<>();
      gmsPropertyResponse
          .getValues()
          .forEach(
              v -> {
                if (v.getString() != null) {
                  values.add(new StringValue(v.getString()));
                } else {
                  values.add(new NumberValue(v.getDouble()));
                }
              });
      propertyResponse.setValues(values);
      result.setStructuredPropertyResponse(propertyResponse);
    }

    if (promptResponse.getOwnershipResponse() != null) {
      com.linkedin.common.OwnershipPromptResponse gmsOwnerResponse =
          promptResponse.getOwnershipResponse();
      OwnershipPromptResponse ownershipPromptResponse = new OwnershipPromptResponse();
      List<Entity> ownerEntities =
          gmsOwnerResponse.getOwners().stream()
              .map(o -> UrnToEntityMapper.map(null, o))
              .collect(Collectors.toList());
      ownershipPromptResponse.setOwners(ownerEntities);
      ownershipPromptResponse.setOwnershipTypeUrn(
          gmsOwnerResponse.getOwnershipTypeUrn().toString());
      result.setOwnershipResponse(ownershipPromptResponse);
    }
    if (promptResponse.getDocumentationResponse() != null) {
      com.linkedin.common.DocumentationPromptResponse gmsDocumentationResponse =
          promptResponse.getDocumentationResponse();
      DocumentationResponse documentationResponse = new DocumentationResponse();
      documentationResponse.setDocumentation(gmsDocumentationResponse.getDocumentation());
      result.setDocumentationResponse(documentationResponse);
    }
    if (promptResponse.getGlossaryTermsResponse() != null) {
      com.linkedin.common.GlossaryTermsPromptResponse gmsGlossaryTermsResponse =
          promptResponse.getGlossaryTermsResponse();
      GlossaryTermsPromptResponse glossaryTermsResponse = new GlossaryTermsPromptResponse();
      List<GlossaryTerm> terms = new ArrayList<>();
      gmsGlossaryTermsResponse
          .getGlossaryTerms()
          .forEach(
              urn -> {
                GlossaryTerm term = new GlossaryTerm();
                term.setType(EntityType.GLOSSARY_TERM);
                term.setUrn(urn.toString());
                terms.add(term);
              });
      glossaryTermsResponse.setGlossaryTerms(terms);
      result.setGlossaryTermsResponse(glossaryTermsResponse);
    }
    if (promptResponse.getDomainResponse() != null) {
      com.linkedin.common.DomainPromptResponse gmsDomainResponse =
          promptResponse.getDomainResponse();
      DomainPromptResponse domainResponse = new DomainPromptResponse();
      Domain domain = new Domain();
      domain.setUrn(gmsDomainResponse.getDomain().toString());
      domain.setType(EntityType.DOMAIN);
      domainResponse.setDomain(domain);
      result.setDomainResponse(domainResponse);
    }

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
          if (fieldFormPromptAssociation.getResponse() != null) {
            association.setResponse(mapPromptResponse(fieldFormPromptAssociation.getResponse()));
          }
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
