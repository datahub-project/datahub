package com.linkedin.datahub.graphql.types.dataset;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.datahub.graphql.generated.DownstreamLineage;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.mappers.DownstreamLineageMapper;
import com.linkedin.dataset.client.Lineages;
import com.linkedin.r2.RemoteInvocationException;

import java.util.List;
import java.util.stream.Collectors;

public class DownstreamLineageType implements LoadableType<DownstreamLineage> {

    private final Lineages _lineageClient;

    public DownstreamLineageType(final Lineages lineageClient) {
        _lineageClient = lineageClient;
    }

    @Override
    public Class<DownstreamLineage> objectClass() {
        return DownstreamLineage.class;
    }

    @Override
    public List<DownstreamLineage> batchLoad(final List<String> keys) {

        final List<DatasetUrn> datasetUrns = keys.stream()
                .map(DatasetUtils::getDatasetUrn)
                .collect(Collectors.toList());

        try {
            return datasetUrns.stream().map(urn -> {
                try {
                    com.linkedin.dataset.DownstreamLineage gmsLineage = _lineageClient.getDownstreamLineage(urn);
                    return DownstreamLineageMapper.map(gmsLineage);
                } catch (RemoteInvocationException e) {
                    throw new RuntimeException(String.format("Failed to batch load DownstreamLineage for dataset %s", urn), e);
                }
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
        }
    }
}
