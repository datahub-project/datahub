package com.linkedin.datahub.graphql.types.dataplatform.mappers;

import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.aspect.DataPlatformAspect;
import com.linkedin.metadata.snapshot.DataPlatformSnapshot;
import javax.annotation.Nonnull;


public class DataPlatformSnapshotMapper implements ModelMapper<DataPlatformSnapshot, DataPlatform> {

    public static final DataPlatformSnapshotMapper INSTANCE = new DataPlatformSnapshotMapper();

    public static DataPlatform map(@Nonnull final DataPlatformSnapshot platform) {
        return INSTANCE.apply(platform);
    }

    @Override
    public DataPlatform apply(@Nonnull final DataPlatformSnapshot input) {
        final DataPlatform result = new DataPlatform();
        result.setType(EntityType.DATA_PLATFORM);
        result.setUrn(input.getUrn().toString());
        result.setName(input.getUrn().getPlatformNameEntity());

        for (DataPlatformAspect aspect : input.getAspects()) {
            if (aspect.isDataPlatformInfo()) {
                result.setInfo(DataPlatformInfoMapper.map(aspect.getDataPlatformInfo()));
            }
        }
        return result;
    }
}
