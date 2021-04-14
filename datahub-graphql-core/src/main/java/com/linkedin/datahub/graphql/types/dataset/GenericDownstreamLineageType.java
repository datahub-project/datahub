package com.linkedin.datahub.graphql.types.dataset;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GenericLineage;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.lineage.mappers.GenericLineageMapper;
import com.linkedin.lineage.client.GenericDownstreamLineages;
import com.linkedin.r2.RemoteInvocationException;

import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

public class GenericDownstreamLineageType implements LoadableType<GenericLineage> {

    private final GenericDownstreamLineages _lineageClient;

    public GenericDownstreamLineageType(final GenericDownstreamLineages lineageClient) {
        _lineageClient = lineageClient;
    }

    @Override
    public Class<GenericLineage> objectClass() {
        return GenericLineage.class;
    }

    @Override
    public List<GenericLineage> batchLoad(final List<String> keys, final QueryContext context) {

        try {
            return keys.stream().map(urn -> {
                try {
                    com.linkedin.common.lineage.GenericLineage genericLineage = _lineageClient.getGenericDownstreamLineage(urn);
                    return GenericLineageMapper.map(genericLineage);
                } catch (RemoteInvocationException | URISyntaxException e) {
                    throw new RuntimeException(String.format("Failed to batch load DownstreamLineage for dataset %s", urn), e);
                }
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
        }
    }
}
