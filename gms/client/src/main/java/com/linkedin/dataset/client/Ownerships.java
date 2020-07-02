package com.linkedin.dataset.client;

import com.linkedin.common.Ownership;
import com.linkedin.common.client.DatasetsClient;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.RawOwnershipRequestBuilders;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.common.urn.UrnUtils.*;

public class Ownerships extends DatasetsClient {

    private static final RawOwnershipRequestBuilders RAW_OWNERSHIP_REQUEST_BUILDERS = new RawOwnershipRequestBuilders();

    public Ownerships(@Nonnull Client restliClient) {
        super(restliClient);
    }

    /**
     * Gets dataset ownership by dataset Urn + version
     *
     * @param datasetUrn DatasetUrn
     * @param version long
     * @return Ownership
     * @throws RemoteInvocationException
     */
    @Nonnull
    public Ownership getOwnership(@Nonnull DatasetUrn datasetUrn, long version)
            throws RemoteInvocationException {
        GetRequest<Ownership> getRequest = RAW_OWNERSHIP_REQUEST_BUILDERS.get()
                .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
                .id(version)
                .build();

        return _client.sendRequest(getRequest).getResponse().getEntity();
    }

    /**
     * Gets latest version of ownership info of a dataset.
     *
     * @param datasetUrn DatasetUrn
     * @return Ownership
     * @throws RemoteInvocationException
     */
    @Nonnull
    public Ownership getLatestOwnership(@Nonnull DatasetUrn datasetUrn) throws RemoteInvocationException {

        GetRequest<Ownership> req = RAW_OWNERSHIP_REQUEST_BUILDERS.get()
                .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
                .id(BaseLocalDAO.LATEST_VERSION)
                .build();

        return _client.sendRequest(req).getResponse().getEntity();
    }

    /**
     * Create an ownership record for a dataset.
     *
     * @param datasetUrn DatasetUrn
     * @param ownership Ownership
     * @param actor Urn
     * @throws RemoteInvocationException
     */
    public void createOwnership(@Nonnull DatasetUrn datasetUrn, @Nonnull Ownership ownership, @Nullable Urn actor)
            throws RemoteInvocationException {

        ownership.setLastModified(getAuditStamp(actor).setTime(System.currentTimeMillis()));

        CreateIdRequest<Long, Ownership> req = RAW_OWNERSHIP_REQUEST_BUILDERS.create()
                .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
                .input(ownership)
                .build();

        _client.sendRequest(req).getResponseEntity();
    }
}
