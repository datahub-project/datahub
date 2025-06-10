package com.linkedin.datahub.graphql.types.form;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssignmentStatus;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainParams;
import com.linkedin.datahub.graphql.generated.DynamicFormAssignment;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.FormActorAssignment;
import com.linkedin.datahub.graphql.generated.FormAssignmentStatus;
import com.linkedin.datahub.graphql.generated.FormInfo;
import com.linkedin.datahub.graphql.generated.FormNotificationSettings;
import com.linkedin.datahub.graphql.generated.FormPrompt;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.FormSettings;
import com.linkedin.datahub.graphql.generated.FormState;
import com.linkedin.datahub.graphql.generated.FormStatus;
import com.linkedin.datahub.graphql.generated.FormType;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.GlossaryTermsParams;
import com.linkedin.datahub.graphql.generated.OwnershipParams;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.datahub.graphql.generated.PromptCardinality;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.StructuredPropertyParams;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FormMapper implements ModelMapper<EntityResponse, Form> {

  public static final FormMapper INSTANCE = new FormMapper();

  public static Form map(@Nullable final QueryContext context, @Nonnull final EntityResponse form) {
    return INSTANCE.apply(context, form);
  }

  public Form apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    Form result = new Form();
    Urn entityUrn = entityResponse.getUrn();
    result.setUrn(entityUrn.toString());
    result.setType(EntityType.FORM);
    // set the default required values for a form in case references are still being cleaned up
    setDefaultForm(result);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<Form> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(FORM_INFO_ASPECT_NAME, this::mapFormInfo);
    mappingHelper.mapToResult(DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME, this::mapDynamicFormAssignment);
    mappingHelper.mapToResult(FORM_ASSIGNMENT_STATUS_ASPECT_NAME, this::mapFormAssignmentStatus);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (form, dataMap) ->
            form.setOwnership(OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(FORM_SETTINGS_ASPECT_NAME, this::mapFormSettings);

    return mappingHelper.getResult();
  }

  private void mapFormInfo(@Nonnull Form form, @Nonnull DataMap dataMap) {
    com.linkedin.form.FormInfo gmsFormInfo = new com.linkedin.form.FormInfo(dataMap);
    FormInfo formInfo = new FormInfo();
    formInfo.setName(gmsFormInfo.getName());
    formInfo.setType(FormType.valueOf(gmsFormInfo.getType().toString()));
    if (gmsFormInfo.hasDescription()) {
      formInfo.setDescription(gmsFormInfo.getDescription());
    }
    formInfo.setPrompts(this.mapFormPrompts(gmsFormInfo, form.getUrn()));
    formInfo.setActors(mapFormActors(gmsFormInfo.getActors()));
    formInfo.setStatus(mapFormStatus(gmsFormInfo.getStatus()));
    if (gmsFormInfo.getCreated() != null) {
      formInfo.setCreated(MapperUtils.createResolvedAuditStamp(gmsFormInfo.getCreated()));
    }
    if (gmsFormInfo.getLastModified() != null) {
      formInfo.setLastModified(MapperUtils.createResolvedAuditStamp(gmsFormInfo.getLastModified()));
    }

    form.setInfo(formInfo);
  }

  private FormStatus mapFormStatus(@Nonnull com.linkedin.form.FormStatus gmsFormStatus) {
    FormStatus formStatus = new FormStatus();
    formStatus.setState(FormState.valueOf(gmsFormStatus.getState().toString()));
    if (gmsFormStatus.getLastModified() != null) {
      formStatus.setLastModified(AuditStampMapper.map(null, gmsFormStatus.getLastModified()));
    }

    return formStatus;
  }

  private List<FormPrompt> mapFormPrompts(
      @Nonnull com.linkedin.form.FormInfo gmsFormInfo, @Nonnull String formUrn) {
    List<FormPrompt> formPrompts = new ArrayList<>();
    if (gmsFormInfo.hasPrompts()) {
      gmsFormInfo
          .getPrompts()
          .forEach(FormPrompt -> formPrompts.add(mapFormPrompt(FormPrompt, formUrn)));
    }
    return formPrompts;
  }

  private FormPrompt mapFormPrompt(
      @Nonnull com.linkedin.form.FormPrompt gmsFormPrompt, @Nonnull String formUrn) {
    final FormPrompt formPrompt = new FormPrompt();
    formPrompt.setId(gmsFormPrompt.getId());
    formPrompt.setTitle(gmsFormPrompt.getTitle());
    formPrompt.setType(FormPromptType.valueOf(gmsFormPrompt.getType().toString()));
    formPrompt.setRequired(gmsFormPrompt.isRequired());
    formPrompt.setFormUrn(formUrn);
    if (gmsFormPrompt.hasDescription()) {
      formPrompt.setDescription(gmsFormPrompt.getDescription());
    }

    if (gmsFormPrompt.hasStructuredPropertyParams()) {
      final StructuredPropertyParams params = new StructuredPropertyParams();
      final Urn structuredPropUrn = gmsFormPrompt.getStructuredPropertyParams().getUrn();
      final StructuredPropertyEntity structuredProp = new StructuredPropertyEntity();
      structuredProp.setUrn(structuredPropUrn.toString());
      structuredProp.setType(EntityType.STRUCTURED_PROPERTY);
      params.setStructuredProperty(structuredProp);
      formPrompt.setStructuredPropertyParams(params);
    }
    if (gmsFormPrompt.getOwnershipParams() != null) {
      formPrompt.setOwnershipParams(mapOwnershipParams(gmsFormPrompt.getOwnershipParams()));
    }

    if (gmsFormPrompt.getGlossaryTermsParams() != null) {
      formPrompt.setGlossaryTermsParams(
          mapGlossaryTermsParams(gmsFormPrompt.getGlossaryTermsParams()));
    }

    if (gmsFormPrompt.getDomainParams() != null) {
      formPrompt.setDomainParams(mapDomainParams(gmsFormPrompt.getDomainParams()));
    }

    return formPrompt;
  }

  private OwnershipParams mapOwnershipParams(com.linkedin.form.OwnershipParams inputParams) {
    final OwnershipParams ownershipParams = new OwnershipParams();
    ownershipParams.setCardinality(
        PromptCardinality.valueOf(inputParams.getCardinality().toString()));
    if (inputParams.getAllowedOwners() != null) {
      List<Entity> allowedOwners =
          inputParams.getAllowedOwners().stream()
              .map(urn -> UrnToEntityMapper.map(null, urn))
              .collect(Collectors.toList());
      ownershipParams.setAllowedOwners(allowedOwners);
    }
    if (inputParams.getAllowedOwnershipTypes() != null) {
      List<OwnershipTypeEntity> allowedOwnershipTypes =
          inputParams.getAllowedOwnershipTypes().stream()
              .map(
                  urn -> {
                    OwnershipTypeEntity termGroup = new OwnershipTypeEntity();
                    termGroup.setUrn(urn.toString());
                    termGroup.setType(EntityType.CUSTOM_OWNERSHIP_TYPE);
                    return termGroup;
                  })
              .collect(Collectors.toList());
      ownershipParams.setAllowedOwnershipTypes(allowedOwnershipTypes);
    }

    return ownershipParams;
  }

  private FormActorAssignment mapFormActors(com.linkedin.form.FormActorAssignment gmsFormActors) {
    FormActorAssignment result = new FormActorAssignment();
    result.setOwners(gmsFormActors.isOwners());
    if (gmsFormActors.hasUsers()) {
      result.setUsers(
          gmsFormActors.getUsers().stream().map(this::mapUser).collect(Collectors.toList()));
    }
    if (gmsFormActors.hasGroups()) {
      result.setGroups(
          gmsFormActors.getGroups().stream().map(this::mapGroup).collect(Collectors.toList()));
    }
    return result;
  }

  private CorpUser mapUser(Urn userUrn) {
    CorpUser user = new CorpUser();
    user.setUrn(userUrn.toString());
    return user;
  }

  private CorpGroup mapGroup(Urn groupUrn) {
    CorpGroup group = new CorpGroup();
    group.setUrn(groupUrn.toString());
    return group;
  }

  private GlossaryTermsParams mapGlossaryTermsParams(
      com.linkedin.form.GlossaryTermsParams inputParams) {
    final GlossaryTermsParams glossaryTermsParams = new GlossaryTermsParams();
    glossaryTermsParams.setCardinality(
        PromptCardinality.valueOf(inputParams.getCardinality().toString()));
    if (inputParams.getAllowedTerms() != null) {
      List<GlossaryTerm> allowedTerms =
          inputParams.getAllowedTerms().stream()
              .map(
                  urn -> {
                    GlossaryTerm term = new GlossaryTerm();
                    term.setUrn(urn.toString());
                    term.setType(EntityType.GLOSSARY_TERM);
                    return term;
                  })
              .collect(Collectors.toList());
      glossaryTermsParams.setAllowedTerms(allowedTerms);
    }
    if (inputParams.getAllowedTermGroups() != null) {
      List<GlossaryNode> allowedTermsGroups =
          inputParams.getAllowedTermGroups().stream()
              .map(
                  urn -> {
                    GlossaryNode termGroup = new GlossaryNode();
                    termGroup.setUrn(urn.toString());
                    termGroup.setType(EntityType.GLOSSARY_NODE);
                    return termGroup;
                  })
              .collect(Collectors.toList());
      glossaryTermsParams.setAllowedTermGroups(allowedTermsGroups);
    }

    return glossaryTermsParams;
  }

  private DomainParams mapDomainParams(com.linkedin.form.DomainParams inputParams) {
    final DomainParams domainParams = new DomainParams();
    if (inputParams.getAllowedDomains() != null) {
      List<Domain> allowedDomains =
          inputParams.getAllowedDomains().stream()
              .map(
                  urn -> {
                    Domain domain = new Domain();
                    domain.setUrn(urn.toString());
                    domain.setType(EntityType.DOMAIN);
                    return domain;
                  })
              .collect(Collectors.toList());
      domainParams.setAllowedDomains(allowedDomains);
    }

    return domainParams;
  }

  private void mapDynamicFormAssignment(@Nonnull Form form, @Nonnull DataMap dataMap) {
    com.linkedin.form.DynamicFormAssignment gmsFormAssignment =
        new com.linkedin.form.DynamicFormAssignment(dataMap);
    DynamicFormAssignment formAssignment = new DynamicFormAssignment();
    if (gmsFormAssignment.getJson() != null) {
      formAssignment.setJson(gmsFormAssignment.getJson());
    } else if (gmsFormAssignment.getFilter() != null) {
      try {
        String json = FilterConverter.convertFilterToJsonPredicate(gmsFormAssignment.getFilter());
        formAssignment.setJson(json);
      } catch (Exception e) {
        log.error("Error when converting dynamic form assignment filter to json predicate: ", e);
      }
    }
    form.setDynamicFormAssignment(formAssignment);
  }

  private void mapFormSettings(@Nonnull Form form, @Nonnull DataMap dataMap) {
    com.linkedin.form.FormSettings gmsFormSettings = new com.linkedin.form.FormSettings(dataMap);
    FormSettings formSettings = new FormSettings();

    com.linkedin.form.FormNotificationSettings gmsFormNotificationSettings =
        gmsFormSettings.getNotificationSettings();
    FormNotificationSettings formNotificationSettings =
        new FormNotificationSettings(gmsFormNotificationSettings.isNotifyAssigneesOnPublish());
    formSettings.setNotificationSettings(formNotificationSettings);

    form.setFormSettings(formSettings);
  }

  private void mapFormAssignmentStatus(@Nonnull Form form, @Nonnull DataMap dataMap) {
    com.linkedin.form.FormAssignmentStatus gmsFormAssignmentStatus =
        new com.linkedin.form.FormAssignmentStatus(dataMap);
    FormAssignmentStatus formAssignmentStatus = new FormAssignmentStatus();
    formAssignmentStatus.setStatus(
        AssignmentStatus.valueOf(gmsFormAssignmentStatus.getStatus().toString()));
    if (gmsFormAssignmentStatus.getTimestamp() != null) {
      formAssignmentStatus.setTimestamp(gmsFormAssignmentStatus.getTimestamp());
    }
    form.setFormAssignmentStatus(formAssignmentStatus);
  }

  /*
   * In the case that a form is deleted and the references haven't been cleaned up yet (this process is async)
   * set a default form to prevent APIs breaking. The UI queries for whether the entity exists and it will
   * be filtered out.
   */
  private void setDefaultForm(final Form result) {
    FormInfo info = new FormInfo();
    info.setName("");
    info.setType(FormType.COMPLETION);
    info.setPrompts(new ArrayList<>());

    FormActorAssignment actors = new FormActorAssignment();
    actors.setOwners(false);
    actors.setIsAssignedToMe(false);
    info.setActors(actors);

    FormStatus status = new FormStatus();
    status.setState(FormState.DRAFT);
    info.setStatus(status);

    result.setInfo(info);
  }
}
