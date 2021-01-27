package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetLineageType;
import com.linkedin.datahub.graphql.generated.RelatedDataset;
import com.linkedin.dataset.Upstream;

import javax.annotation.Nonnull;

public class UpstreamMapper implements ModelMapper<Upstream, RelatedDataset> {

    public static final UpstreamMapper INSTANCE = new UpstreamMapper();

    public static RelatedDataset map(@Nonnull final Upstream upstream) {
        return INSTANCE.apply(upstream);
    }

    @Override
    public RelatedDataset apply(@Nonnull final Upstream upstream) {
        final RelatedDataset result = new RelatedDataset();

        final Dataset partialDataset = new Dataset();
        partialDataset.setUrn(upstream.getDataset().toString());
        result.setDataset(partialDataset);

        result.setType(DatasetLineageType.valueOf(upstream.getType().toString()));
        result.setCreated(AuditStampMapper.map(upstream.getAuditStamp()));
        return result;
    }
}
