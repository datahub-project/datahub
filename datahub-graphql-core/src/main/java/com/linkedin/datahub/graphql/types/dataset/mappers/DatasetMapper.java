package com.linkedin.datahub.graphql.types.dataset.mappers;

import javax.annotation.Nonnull;

import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.PlatformNativeType;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class DatasetMapper implements ModelMapper<com.linkedin.dataset.Dataset, Dataset> {

    public static final DatasetMapper INSTANCE = new DatasetMapper();

    public static Dataset map(@Nonnull final com.linkedin.dataset.Dataset dataset) {
        return INSTANCE.apply(dataset);
    }

    @Override
    public Dataset apply(@Nonnull final com.linkedin.dataset.Dataset dataset) {
        com.linkedin.datahub.graphql.generated.Dataset result = new com.linkedin.datahub.graphql.generated.Dataset();
        result.setUrn(dataset.getUrn().toString());
        result.setType(EntityType.DATASET);
        result.setName(dataset.getName());
        result.setDescription(dataset.getDescription());
        result.setOrigin(Enum.valueOf(FabricType.class, dataset.getOrigin().name()));
        result.setTags(dataset.getTags());

        DataPlatform partialPlatform = new DataPlatform();
        partialPlatform.setUrn(dataset.getPlatform().toString());
        result.setPlatform(partialPlatform);

        if (dataset.hasSchemaMetadata()) {
            result.setSchema(SchemaMetadataMapper.map(dataset.getSchemaMetadata()));
        }
        if (dataset.hasPlatformNativeType()) {
            result.setPlatformNativeType(Enum.valueOf(PlatformNativeType.class, dataset.getPlatformNativeType().name()));
        }
        if (dataset.hasUri()) {
            result.setUri(dataset.getUri().toString());
        }
        if (dataset.hasProperties()) {
            result.setProperties(StringMapMapper.map(dataset.getProperties()));
        }
        if (dataset.hasOwnership()) {
            result.setOwnership(OwnershipMapper.map(dataset.getOwnership()));
        }
        if (dataset.hasDeprecation()) {
            result.setDeprecation(DatasetDeprecationMapper.map(dataset.getDeprecation()));
        }
        if (dataset.hasInstitutionalMemory()) {
            result.setInstitutionalMemory(InstitutionalMemoryMapper.map(dataset.getInstitutionalMemory()));
        }
        if (dataset.hasStatus()) {
            result.setStatus(StatusMapper.map(dataset.getStatus()));
        }
        if (dataset.hasUpstreamLineage()) {
            result.setUpstreamLineage(UpstreamLineageMapper.map(dataset.getUpstreamLineage()));
        }
        if (dataset.hasGlobalTags()) {
            result.setGlobalTags(GlobalTagsMapper.map(dataset.getGlobalTags()));
        }
        return result;
    }
}
