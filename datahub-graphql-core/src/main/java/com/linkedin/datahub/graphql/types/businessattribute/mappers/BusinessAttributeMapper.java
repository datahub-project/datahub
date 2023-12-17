package com.linkedin.datahub.graphql.types.businessattribute.mappers;

import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.common.Ownership;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;

import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

public class BusinessAttributeMapper implements ModelMapper<EntityResponse, BusinessAttribute> {

    public static final BusinessAttributeMapper INSTANCE = new BusinessAttributeMapper();

    public static BusinessAttribute map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public BusinessAttribute apply(@Nonnull final EntityResponse entityResponse) {
        BusinessAttribute result = new BusinessAttribute();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.BUSINESS_ATTRIBUTE);

        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<BusinessAttribute> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME, ((businessAttribute, dataMap) ->
                mapBusinessAttributeInfo(businessAttribute, dataMap)));
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (businessAttribute, dataMap) ->
                businessAttribute.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityResponse.getUrn())));
        return mappingHelper.getResult();
    }

    private void mapBusinessAttributeInfo(BusinessAttribute businessAttribute, DataMap dataMap) {
        BusinessAttributeInfo businessAttributeInfo = new BusinessAttributeInfo(dataMap);
        com.linkedin.datahub.graphql.generated.BusinessAttributeInfo attributeInfo = new com.linkedin.datahub.graphql.generated.BusinessAttributeInfo();
        if (businessAttributeInfo.hasFieldPath()) {
            attributeInfo.setName(businessAttributeInfo.getFieldPath());
        }
        if (businessAttributeInfo.hasDescription()) {
            attributeInfo.setDescription(businessAttributeInfo.getDescription());
        }
        if (businessAttributeInfo.hasCreated()) {
            attributeInfo.setCreated(AuditStampMapper.map(businessAttributeInfo.getCreated()));
        }
        if (businessAttributeInfo.hasLastModified()) {
            attributeInfo.setLastModified(AuditStampMapper.map(businessAttributeInfo.getLastModified()));
        }
        if (businessAttributeInfo.hasGlobalTags()) {

        }
        if (businessAttributeInfo.hasGlossaryTerms()) {

        }
        businessAttribute.setBusinessAttributeInfo(attributeInfo);
    }

}
