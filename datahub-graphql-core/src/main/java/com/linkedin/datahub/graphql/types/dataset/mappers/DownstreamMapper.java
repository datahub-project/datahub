package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetLineageType;
import com.linkedin.datahub.graphql.generated.RelatedDataset;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.dataset.Downstream;

import javax.annotation.Nonnull;

public class DownstreamMapper implements ModelMapper<Downstream, RelatedDataset> {

    public static final DownstreamMapper INSTANCE = new DownstreamMapper();

    public static RelatedDataset map(@Nonnull final Downstream downstream) {
        return INSTANCE.apply(downstream);
    }

    @Override
    public RelatedDataset apply(@Nonnull final Downstream downstream) {
        final RelatedDataset result = new RelatedDataset();

        final Dataset partialDataset = new Dataset();
        partialDataset.setUrn(downstream.getDataset().toString());
        result.setDataset(partialDataset);

        result.setType(DatasetLineageType.valueOf(downstream.getType().toString()));
        result.setCreated(AuditStampMapper.map(downstream.getAuditStamp()));
        return result;
    }
}
