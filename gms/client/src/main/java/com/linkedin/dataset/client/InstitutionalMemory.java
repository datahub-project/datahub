package com.linkedin.dataset.client;

import com.linkedin.common.client.DatasetsClient;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.InstitutionalMemoryRequestBuilders;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import javax.annotation.Nonnull;

public class InstitutionalMemory extends DatasetsClient {

    private static final InstitutionalMemoryRequestBuilders INSTITUTIONAL_MEMORY_REQUEST_BUILDERS
            = new InstitutionalMemoryRequestBuilders();

    public InstitutionalMemory(@Nonnull Client restliClient) {
        super(restliClient);
    }

    /**
     * Creates or Updates InstitutionalMemory aspect
     *
     * @param datasetUrn dataset urn
     * @param institutionalMemory institutional memory
     */
    public void updateInstitutionalMemory(@Nonnull DatasetUrn datasetUrn,
                                          @Nonnull com.linkedin.common.InstitutionalMemory institutionalMemory)
            throws RemoteInvocationException {

        CreateIdRequest<Long, com.linkedin.common.InstitutionalMemory> request = INSTITUTIONAL_MEMORY_REQUEST_BUILDERS.create()
                .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
                .input(institutionalMemory)
                .build();

        _client.sendRequest(request).getResponse();
    }

    /**
     * Get InstitutionalMemory aspect
     *
     * @param datasetUrn dataset urn
     */
    @Nonnull
    public com.linkedin.common.InstitutionalMemory getInstitutionalMemory(@Nonnull DatasetUrn datasetUrn)
            throws RemoteInvocationException {

        Request<com.linkedin.common.InstitutionalMemory> request =
                INSTITUTIONAL_MEMORY_REQUEST_BUILDERS.get()
                        .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
                        .id(BaseLocalDAO.LATEST_VERSION)
                        .build();

        return _client.sendRequest(request).getResponse().getEntity();
    }
}
