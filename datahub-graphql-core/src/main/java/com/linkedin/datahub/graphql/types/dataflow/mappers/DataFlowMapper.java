package com.linkedin.datahub.graphql.types.dataflow.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataFlowEditableProperties;
import com.linkedin.datahub.graphql.generated.DataFlowInfo;
import com.linkedin.datahub.graphql.generated.DataFlowProperties;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.datajob.EditableDataFlowProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.key.DataFlowKey;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class DataFlowMapper implements ModelMapper<EntityResponse, DataFlow> {

    public static final DataFlowMapper INSTANCE = new DataFlowMapper();

    public static DataFlow map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public DataFlow apply(@Nonnull final EntityResponse entityResponse) {
        final DataFlow result = new DataFlow();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.DATA_FLOW);

        entityResponse.getAspects().forEach((name, aspect) -> {
            DataMap data = aspect.getValue().data();
            if (DATA_FLOW_KEY_ASPECT_NAME.equals(name)) {
                final DataFlowKey gmsKey = new DataFlowKey(data);
                result.setOrchestrator(gmsKey.getOrchestrator());
                result.setFlowId(gmsKey.getFlowId());
                result.setCluster(gmsKey.getCluster());
            } else if (DATA_FLOW_INFO_ASPECT_NAME.equals(name)) {
                final com.linkedin.datajob.DataFlowInfo gmsDataFlowInfo = new com.linkedin.datajob.DataFlowInfo(data);
                result.setInfo(mapDataFlowInfo(gmsDataFlowInfo));
                result.setProperties(mapDataFlowInfoToProperties(gmsDataFlowInfo));
            } else if (EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME.equals(name)) {
                final EditableDataFlowProperties editableDataFlowProperties = new EditableDataFlowProperties(data);
                final DataFlowEditableProperties dataFlowEditableProperties = new DataFlowEditableProperties();
                dataFlowEditableProperties.setDescription(editableDataFlowProperties.getDescription());
                result.setEditableProperties(dataFlowEditableProperties);
            } else if (OWNERSHIP_ASPECT_NAME.equals(name)) {
                result.setOwnership(OwnershipMapper.map(new Ownership(data)));
            } else if (STATUS_ASPECT_NAME.equals(name)) {
                result.setStatus(StatusMapper.map(new Status(data)));
            } else if (GLOBAL_TAGS_ASPECT_NAME.equals(name)) {
                com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(data));
                result.setGlobalTags(globalTags);
                result.setTags(globalTags);
            } else if (INSTITUTIONAL_MEMORY_ASPECT_NAME.equals(name)) {
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(data)));
            } else if (GLOSSARY_TERMS_ASPECT_NAME.equals(name)) {
                result.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(data)));
            }
        });

        return result;
    }

    /**
     * Maps GMS {@link com.linkedin.datajob.DataFlowInfo} to deprecated GraphQL {@link DataFlowInfo}
     */
    private DataFlowInfo mapDataFlowInfo(final com.linkedin.datajob.DataFlowInfo info) {
        final DataFlowInfo result = new DataFlowInfo();
        result.setName(info.getName());
        result.setDescription(info.getDescription());
        result.setProject(info.getProject());
        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        return result;
    }

    /**
     * Maps GMS {@link com.linkedin.datajob.DataFlowInfo} to new GraphQL {@link DataFlowProperties}
     */
    private DataFlowProperties mapDataFlowInfoToProperties(final com.linkedin.datajob.DataFlowInfo info) {
        final DataFlowProperties result = new DataFlowProperties();
        result.setName(info.getName());
        result.setDescription(info.getDescription());
        result.setProject(info.getProject());
        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        return result;
    }
}
