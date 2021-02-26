package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import javax.annotation.Nonnull;

public class DataPlatformMapper implements ModelMapper<com.linkedin.dataPlatforms.DataPlatform, DataPlatform> {

    public static final DataPlatformMapper INSTANCE = new DataPlatformMapper();

    public static DataPlatform map(@Nonnull final com.linkedin.dataPlatforms.DataPlatform platform) {
        return INSTANCE.apply(platform);
    }

    @Override
    public DataPlatform apply(@Nonnull final com.linkedin.dataPlatforms.DataPlatform input) {
        final DataPlatform result = new DataPlatform();
        result.setUrn(new DataPlatformUrn(input.getName()).toString());
        result.setName(input.getName());
        if (input.hasDataPlatformInfo()) {
            result.setInfo(DataPlatformInfoMapper.map(input.getDataPlatformInfo()));
        }
        return result;
    }
}
