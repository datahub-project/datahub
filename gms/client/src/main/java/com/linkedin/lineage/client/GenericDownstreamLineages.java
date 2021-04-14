package com.linkedin.lineage.client;

import com.linkedin.common.client.BaseClient;
import com.linkedin.common.lineage.GenericLineage;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.ChartKey;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.dataset.GenericDownstreamLineageRequestBuilders;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import javax.annotation.Nonnull;
import java.net.URISyntaxException;

public class GenericDownstreamLineages extends BaseClient {

    public GenericDownstreamLineages(@Nonnull Client restliClient) {
        super(restliClient);
    }
    private static final GenericDownstreamLineageRequestBuilders DATASET_GENERIC_DOWNSTREAM_LINEAGE_REQUEST_BUILDERS =
            new GenericDownstreamLineageRequestBuilders();

    private static final com.linkedin.chart.GenericDownstreamLineageRequestBuilders CHART_GENERIC_DOWNSTREAM_LINEAGE_REQUEST_BUILDERS =
            new com.linkedin.chart.GenericDownstreamLineageRequestBuilders();

    protected DatasetKey toDatasetKey(@Nonnull DatasetUrn urn) {
        return new DatasetKey()
                .setName(urn.getDatasetNameEntity())
                .setOrigin(urn.getOriginEntity())
                .setPlatform(urn.getPlatformEntity());
    }

    @Nonnull
    private ChartKey toChartKey(@Nonnull ChartUrn urn) {
        return new ChartKey().setTool(urn.getDashboardToolEntity()).setChartId(urn.getChartIdEntity());
    }

    /**
     * Gets a specific version of downstream {@link com.linkedin.common.lineage.GenericLineage} for the given dataset.
     */
    @Nonnull
    public GenericLineage getGenericDownstreamLineage(@Nonnull String rawUrn)
            throws RemoteInvocationException, URISyntaxException {

        Urn urn = Urn.createFromString(rawUrn);

        if (urn.getEntityType().equals("dataset")) {
            final GetRequest<GenericLineage> request = DATASET_GENERIC_DOWNSTREAM_LINEAGE_REQUEST_BUILDERS.get()
                    .datasetKey(new ComplexResourceKey<>(toDatasetKey(DatasetUrn.createFromUrn(urn)), new EmptyRecord()))
                    .build();
            return _client.sendRequest(request).getResponseEntity();
        }
        if (urn.getEntityType().equals("chart")) {
            final GetRequest<GenericLineage> request = CHART_GENERIC_DOWNSTREAM_LINEAGE_REQUEST_BUILDERS.get()
                    .keyKey(new ComplexResourceKey<>(toChartKey(ChartUrn.createFromUrn(urn)), new EmptyRecord()))
                    .build();
            return _client.sendRequest(request).getResponseEntity();
        }
        return null;
    }
}
