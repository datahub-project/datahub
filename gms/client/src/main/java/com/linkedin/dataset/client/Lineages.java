package com.linkedin.dataset.client;

import com.linkedin.common.client.DatasetsClient;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DownstreamLineage;
import com.linkedin.dataset.DownstreamLineageRequestBuilders;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.dataset.UpstreamLineageDelta;
import com.linkedin.dataset.UpstreamLineageRequestBuilders;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.ActionRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import javax.annotation.Nonnull;

public class Lineages extends DatasetsClient {

    private static final UpstreamLineageRequestBuilders UPSTREAM_LINEAGE_REQUEST_BUILDERS =
        new UpstreamLineageRequestBuilders();
    private static final DownstreamLineageRequestBuilders DOWNSTREAM_LINEAGE_REQUEST_BUILDERS =
        new DownstreamLineageRequestBuilders();

    public Lineages(@Nonnull Client restliClient) {
        super(restliClient);
    }

    /**
     * Gets a specific version of {@link UpstreamLineage} for the given dataset.
     */
    @Nonnull
    public UpstreamLineage getUpstreamLineage(@Nonnull DatasetUrn datasetUrn, long version)
            throws RemoteInvocationException {

        final GetRequest<UpstreamLineage> request = UPSTREAM_LINEAGE_REQUEST_BUILDERS.get()
                .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
                .id(version)
                .build();
        return _client.sendRequest(request).getResponseEntity();
    }

    /**
     * Gets a specific version of {@link DownstreamLineage} for the given dataset.
     */
    @Nonnull
    public DownstreamLineage getDownstreamLineage(@Nonnull DatasetUrn datasetUrn)
        throws RemoteInvocationException {

        final GetRequest<DownstreamLineage> request = DOWNSTREAM_LINEAGE_REQUEST_BUILDERS.get()
            .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
            .build();
        return _client.sendRequest(request).getResponseEntity();
    }

    /**
     * Similar to {@link #getUpstreamLineage(DatasetUrn)} but returns the latest version.
     */
    @Nonnull
    public UpstreamLineage getUpstreamLineage(@Nonnull DatasetUrn datasetUrn) throws RemoteInvocationException {
        return getUpstreamLineage(datasetUrn, BaseLocalDAO.LATEST_VERSION);
    }

    /**
     * Sets {@link UpstreamLineage} for a specific dataset.
     */
    public void setUpstreamLineage(@Nonnull DatasetUrn datasetUrn, @Nonnull UpstreamLineage upstreamLineage)
            throws RemoteInvocationException {

        final CreateIdRequest<Long, UpstreamLineage> request = UPSTREAM_LINEAGE_REQUEST_BUILDERS.create()
                .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
                .input(upstreamLineage)
                .build();
        _client.sendRequest(request).getResponseEntity();
    }

    /**
     * Delta update for {@link UpstreamLineage}
     *
     * @param datasetUrn datasetUrn
     * @param delta upstreamDelta
     * @return updated {@link UpstreamLineage}
     * @throws RemoteInvocationException throws RemoteInvocationException
     */
    @Nonnull
    public UpstreamLineage deltaUpdateUpstreamLineage(@Nonnull DatasetUrn datasetUrn, @Nonnull UpstreamLineageDelta delta)
            throws RemoteInvocationException {

        final ActionRequest<UpstreamLineage> request = UPSTREAM_LINEAGE_REQUEST_BUILDERS.actionDeltaUpdate()
                .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
                .deltaParam(delta)
                .build();
        return _client.sendRequest(request).getResponseEntity();
    }
}
