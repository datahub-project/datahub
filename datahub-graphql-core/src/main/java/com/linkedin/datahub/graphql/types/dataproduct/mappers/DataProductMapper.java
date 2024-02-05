package com.linkedin.datahub.graphql.types.dataproduct.mappers;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;

public class DataProductMapper implements ModelMapper<EntityResponse, DataProduct> {

  public static final DataProductMapper INSTANCE = new DataProductMapper();

  public static DataProduct map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public DataProduct apply(@Nonnull final EntityResponse entityResponse) {
    final DataProduct result = new DataProduct();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATA_PRODUCT);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<DataProduct> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
        (dataProduct, dataMap) -> mapDataProductProperties(dataProduct, dataMap, entityUrn));
    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (dataProduct, dataMap) ->
            dataProduct.setTags(GlobalTagsMapper.map(new GlobalTags(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (dataProduct, dataMap) ->
            dataProduct.setGlossaryTerms(
                GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        DOMAINS_ASPECT_NAME,
        (dataProduct, dataMap) ->
            dataProduct.setDomain(
                DomainAssociationMapper.map(new Domains(dataMap), dataProduct.getUrn())));
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (dataProduct, dataMap) ->
            dataProduct.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (dataProduct, dataMap) ->
            dataProduct.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap), entityUrn)));

    return result;
  }

  private void mapDataProductProperties(
      @Nonnull DataProduct dataProduct, @Nonnull DataMap dataMap, @Nonnull Urn urn) {
    DataProductProperties dataProductProperties = new DataProductProperties(dataMap);
    com.linkedin.datahub.graphql.generated.DataProductProperties properties =
        new com.linkedin.datahub.graphql.generated.DataProductProperties();

    final String name =
        dataProductProperties.hasName() ? dataProductProperties.getName() : urn.getId();
    properties.setName(name);
    properties.setDescription(dataProductProperties.getDescription());
    if (dataProductProperties.hasExternalUrl()) {
      properties.setExternalUrl(dataProductProperties.getExternalUrl().toString());
    }
    if (dataProductProperties.hasAssets()) {
      properties.setNumAssets(dataProductProperties.getAssets().size());
    } else {
      properties.setNumAssets(0);
    }
    properties.setCustomProperties(
        CustomPropertiesMapper.map(
            dataProductProperties.getCustomProperties(), UrnUtils.getUrn(dataProduct.getUrn())));

    dataProduct.setProperties(properties);
  }
}
