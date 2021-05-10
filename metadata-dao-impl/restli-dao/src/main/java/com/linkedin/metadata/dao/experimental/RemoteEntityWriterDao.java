package com.linkedin.metadata.dao.experimental;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.RestliClientException;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;

import javax.annotation.Nonnull;

public class RemoteEntityWriterDao {

    protected final Client _restliClient;

    public RemoteEntityWriterDao(@Nonnull Client restliClient) {
        _restliClient = restliClient;
    }

    public void create(@Nonnull Urn urn, @Nonnull RecordTemplate snapshot)
            throws IllegalArgumentException, RestliClientException {

        final Request request = null; //createRequest(urn, snapshot);

        try {
            _restliClient.sendRequest(request).getResponse();
        } catch (RemoteInvocationException e) {
            throw new RestliClientException(e);
        }
    }

    /*
    private Request createRequest(Urn urn, RecordTemplate snapshot) {
        final ActionRequestBuilder builder = new ActionRequestBuilder(_baseUriTemplate, Void.class, _resourceSpec, RestliRequestOptions.DEFAULT_OPTIONS);
        builder.name(ACTION_INGEST);
        builder.addParam(_snapshotFieldDef, snapshot);
        pathKeys(urn).forEach(builder::pathKey);

        return builder.build();
    }
     */
}
