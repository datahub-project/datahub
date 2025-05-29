package com.linkedin.datahub.graphql.types.application.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.APPLICATION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.FORMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.linkedin.application.ApplicationProperties;
import com.linkedin.common.Forms;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Application;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ApplicationMapper implements ModelMapper<EntityResponse, Application> {

  public static final ApplicationMapper INSTANCE = new ApplicationMapper();

  public static Application map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public Application apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final Application result = new Application();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.APPLICATION);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<Application> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        APPLICATION_PROPERTIES_ASPECT_NAME,
        (application, dataMap) -> mapApplicationProperties(application, dataMap, entityUrn));
    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (application, dataMap) ->
            application.setTags(GlobalTagsMapper.map(context, new GlobalTags(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (application, dataMap) ->
            application.setGlossaryTerms(
                GlossaryTermsMapper.map(context, new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        DOMAINS_ASPECT_NAME,
        (application, dataMap) ->
            application.setDomain(
                DomainAssociationMapper.map(context, new Domains(dataMap), application.getUrn())));
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (application, dataMap) ->
            application.setOwnership(
                OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (application, dataMap) ->
            application.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(
                    context, new InstitutionalMemory(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setStructuredProperties(
                StructuredPropertiesMapper.map(
                    context, new StructuredProperties(dataMap), entityUrn))));
    mappingHelper.mapToResult(
        FORMS_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setForms(FormsMapper.map(new Forms(dataMap), entityUrn.toString()))));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(result, Application.class);
    } else {
      return result;
    }
  }

  private void mapApplicationProperties(
      @Nonnull Application application, @Nonnull DataMap dataMap, @Nonnull Urn urn) {
    ApplicationProperties applicationProperties = new ApplicationProperties(dataMap);
    com.linkedin.datahub.graphql.generated.ApplicationProperties properties =
        new com.linkedin.datahub.graphql.generated.ApplicationProperties();

    final String name =
        applicationProperties.hasName() ? applicationProperties.getName() : urn.getId();
    properties.setName(name);
    properties.setDescription(applicationProperties.getDescription());
    if (applicationProperties.hasExternalUrl()) {
      properties.setExternalUrl(applicationProperties.getExternalUrl().toString());
    }
    if (applicationProperties.hasAssets()) {
      properties.setNumAssets(applicationProperties.getAssets().size());
    } else {
      properties.setNumAssets(0);
    }
    properties.setCustomProperties(
        CustomPropertiesMapper.map(
            applicationProperties.getCustomProperties(), UrnUtils.getUrn(application.getUrn())));

    application.setProperties(properties);
  }
}
