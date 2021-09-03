package com.linkedin.lineage.client;

import com.linkedin.common.EntityRelationships;
import com.linkedin.common.client.BaseClient;
import com.linkedin.lineage.LineageGetRequestBuilder;
import com.linkedin.lineage.LineageRequestBuilders;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;

import javax.annotation.Nonnull;
import java.net.URISyntaxException;

public class Lineages extends BaseClient {

    public Lineages(@Nonnull Client restliClient) {
        super(restliClient);
    }
    private static final LineageRequestBuilders LINEAGE_REQUEST_BUILDERS =
            new LineageRequestBuilders();

    /**
     * Gets a specific version of downstream {@link com.linkedin.common.EntityRelationships} for the given dataset.
     */
    @Nonnull
    public EntityRelationships getLineage(
        @Nonnull String rawUrn,
        @Nonnull RelationshipDirection direction,
        @Nonnull String actor)
            throws RemoteInvocationException, URISyntaxException {
        final LineageGetRequestBuilder requestBuilder = LINEAGE_REQUEST_BUILDERS.get()
                .urnParam(rawUrn)
                .directionParam(direction.toString());
        return sendClientRequest(requestBuilder, actor).getEntity();
    }
}
