package com.linkedin.entity.client;

import com.linkedin.entity.AspectsDoGetAspectValuesRequestBuilder;
import com.linkedin.entity.AspectsGetRequestBuilder;
import com.linkedin.entity.AspectsRequestBuilders;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.query.Filter;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AspectClient {

    private static final AspectsRequestBuilders ASPECTS_REQUEST_BUILDERS = new AspectsRequestBuilders();

    private final Client _client;
    private final Logger _logger = LoggerFactory.getLogger("AspectClient");

    public AspectClient(@Nonnull final Client restliClient) {
        _client = restliClient;
    }

    /**
     * Gets aspect at veresion for an entity
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException
     */
    @Nonnull
    public VersionedAspect getAspect(@Nonnull String urn, @Nonnull String aspect, @Nonnull Long version)
        throws RemoteInvocationException {

        AspectsGetRequestBuilder requestBuilder =
            ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);

        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();

    }

    /**
     * Retrieve instances of a particular aspect.
     *
     * @param urn urn for the entity
     *            TODO: Fill out other arguments.
     * @throws RemoteInvocationException on remote request error.
     */
    @Nonnull
    public List<EnvelopedAspect> getAspectValues(
        @Nonnull String urn,
        @Nonnull String entity,
        @Nonnull String aspect,
        @Nullable Filter filter,
        @Nonnull Integer limit)
        throws RemoteInvocationException {

        AspectsDoGetAspectValuesRequestBuilder requestBuilder = ASPECTS_REQUEST_BUILDERS.actionGetAspectValues()
            .urnParam(urn)
            .entityParam(entity)
            .aspectParam(aspect)
            .filterParam(filter)
            .limitParam(limit);

        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity().getValues();

    }
}
