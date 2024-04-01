package com.linkedin.datahub.graphql.types.form;

import static com.linkedin.metadata.Constants.FORM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.FormActorAssignment;
import com.linkedin.datahub.graphql.generated.FormInfo;
import com.linkedin.datahub.graphql.generated.FormPrompt;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.FormType;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.StructuredPropertyParams;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<Form> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(FORM_INFO_ASPECT_NAME, this::mapFormInfo);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (form, dataMap) ->
            form.setOwnership(OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));

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
    form.setInfo(formInfo);
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

    return formPrompt;
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
}
