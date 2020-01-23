package com.linkedin.dataset.client;

import com.linkedin.common.client.DatasetsClient;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.SchemaRequestBuilders;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.schema.SchemaMetadata;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Schemas extends DatasetsClient {

    private static final SchemaRequestBuilders SCHEMA_REQUEST_BUILDERS = new SchemaRequestBuilders();

    public Schemas(@Nonnull Client restliClient) {
        super(restliClient);
    }

    /**
     * Get latest version of schema associated with a dataset Urn
     * @param datasetUrn DatasetUrn
     * @return SchemaMetadata
     * @throws RemoteInvocationException
     */
    @Nullable
    public SchemaMetadata getLatestSchemaByDataset(@Nonnull DatasetUrn datasetUrn) throws RemoteInvocationException {
        GetRequest<SchemaMetadata> req = SCHEMA_REQUEST_BUILDERS.get()
                .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
                .id(BaseLocalDAO.LATEST_VERSION)
                .build();

        return _client.sendRequest(req).getResponse().getEntity();
    }
}
