package com.linkedin.datahub.graphql.types.api.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.API_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.API_SIGNATURE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.REST_API_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SUB_TYPES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Api;
import com.linkedin.datahub.graphql.generated.ApiProperties;
import com.linkedin.datahub.graphql.generated.ApiSignature;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.HttpMethod;
import com.linkedin.datahub.graphql.generated.RestApiProperties;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.SubTypesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaFieldMapper;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.datahub.graphql.types.versioning.VersionPropertiesMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.structured.StructuredProperties;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ApiMapper implements ModelMapper<EntityResponse, Api> {

  public static final ApiMapper INSTANCE = new ApiMapper();

  public static Api map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public Api apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final Api result = new Api();
    final Urn entityUrn = entityResponse.getUrn();
    result.setUrn(entityUrn.toString());
    result.setType(EntityType.API);

    final EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    final MappingHelper<Api> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        API_PROPERTIES_ASPECT_NAME, (api, dataMap) -> mapApiProperties(api, dataMap, entityUrn));
    mappingHelper.mapToResult(
        API_SIGNATURE_ASPECT_NAME,
        (api, dataMap) -> mapApiSignature(context, api, dataMap, entityUrn));
    mappingHelper.mapToResult(REST_API_PROPERTIES_ASPECT_NAME, ApiMapper::mapRestApiProperties);
    mappingHelper.mapToResult(
        SUB_TYPES_ASPECT_NAME,
        (api, dataMap) -> api.setSubTypes(SubTypesMapper.map(context, new SubTypes(dataMap))));
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (api, dataMap) ->
            api.setOwnership(OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (api, dataMap) ->
            api.setTags(GlobalTagsMapper.map(context, new GlobalTags(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (api, dataMap) ->
            api.setGlossaryTerms(
                GlossaryTermsMapper.map(context, new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        DOMAINS_ASPECT_NAME,
        (api, dataMap) ->
            api.setDomain(
                DomainAssociationMapper.map(context, new Domains(dataMap), entityUrn.toString())));
    mappingHelper.mapToResult(
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (api, dataMap) ->
            api.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(
                    context, new InstitutionalMemory(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (api, dataMap) -> api.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        (api, dataMap) ->
            api.setStructuredProperties(
                StructuredPropertiesMapper.map(
                    context, new StructuredProperties(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        VERSION_PROPERTIES_ASPECT_NAME,
        (api, dataMap) ->
            api.setVersionProperties(
                VersionPropertiesMapper.map(context, new VersionProperties(dataMap))));
    mappingHelper.mapToResult(
        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        (api, dataMap) ->
            api.setDataPlatformInstance(
                DataPlatformInstanceAspectMapper.map(context, new DataPlatformInstance(dataMap))));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(result, Api.class);
    }
    return result;
  }

  private static void mapApiProperties(
      @Nonnull final Api api, @Nonnull final DataMap dataMap, @Nonnull final Urn entityUrn) {
    final com.linkedin.api.ApiProperties info = new com.linkedin.api.ApiProperties(dataMap);
    final ApiProperties properties = new ApiProperties();
    properties.setName(info.hasName() ? info.getName() : entityUrn.getId());
    if (info.hasDescription()) {
      properties.setDescription(info.getDescription());
    }
    if (info.hasExternalUrl()) {
      properties.setExternalUrl(info.getExternalUrl());
    }
    api.setProperties(properties);
  }

  private static void mapApiSignature(
      @Nullable final QueryContext context,
      @Nonnull final Api api,
      @Nonnull final DataMap dataMap,
      @Nonnull final Urn entityUrn) {
    final com.linkedin.api.ApiSignature info = new com.linkedin.api.ApiSignature(dataMap);
    final ApiSignature signature = new ApiSignature();
    if (info.hasSchemaDefinition()) {
      signature.setSchemaDefinition(info.getSchemaDefinition());
    }
    if (info.hasInputFields()) {
      signature.setInputFields(
          info.getInputFields().stream()
              .map(field -> SchemaFieldMapper.map(context, field, entityUrn))
              .collect(Collectors.toList()));
    }
    if (info.hasOutputFields()) {
      signature.setOutputFields(
          info.getOutputFields().stream()
              .map(field -> SchemaFieldMapper.map(context, field, entityUrn))
              .collect(Collectors.toList()));
    }
    api.setSignature(signature);
  }

  private static void mapRestApiProperties(@Nonnull final Api api, @Nonnull final DataMap dataMap) {
    final com.linkedin.api.RestApiProperties info = new com.linkedin.api.RestApiProperties(dataMap);
    // Both fields are required by the PDL, but a partial aspect must degrade to "no REST
    // properties" rather than fail the whole batch load (method/path are non-null in GraphQL).
    if (!info.hasMethod() || !info.hasPath()) {
      return;
    }
    final RestApiProperties result = new RestApiProperties();
    // PdlEnumMapper (not Enum.valueOf) so an unknown/$UNKNOWN PDL method value
    // degrades to a default instead of throwing.
    result.setMethod(PdlEnumMapper.map(HttpMethod.class, info.getMethod(), HttpMethod.GET));
    result.setPath(info.getPath());
    api.setRestProperties(result);
  }
}
