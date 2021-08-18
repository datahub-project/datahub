package com.linkedin.lineage.client;

import com.linkedin.common.EntityRelationships;
import com.linkedin.common.client.BaseClient;
import com.linkedin.lineage.RelationshipsGetRequestBuilder;
import com.linkedin.lineage.RelationshipsRequestBuilders;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;

import java.util.List;
import javax.annotation.Nonnull;
import java.net.URISyntaxException;
import javax.annotation.Nullable;


public class RelationshipClient extends BaseClient {

    public RelationshipClient(@Nonnull Client restliClient) {
        super(restliClient);
    }
    private static final RelationshipsRequestBuilders RELATIONSHIPS_REQUEST_BUILDERS =
            new RelationshipsRequestBuilders();

    /**
     * Gets a specific version of downstream {@link EntityRelationships} for the given dataset.
     */
    @Nonnull
    public EntityRelationships getRelationships(
        @Nonnull String rawUrn,
        @Nonnull RelationshipDirection direction,
        @Nonnull List<String> types,
        @Nullable Integer start,
        @Nullable Integer count,
        @Nonnull String actor)
            throws RemoteInvocationException, URISyntaxException {
        final RelationshipsGetRequestBuilder requestBuilder = RELATIONSHIPS_REQUEST_BUILDERS.get()
                .urnParam(rawUrn)
                .directionParam(direction.toString())
                .typesParam(types);
        if (start != null) {
            requestBuilder.startParam(start);
        }
        if (count != null) {
            requestBuilder.countParam(count);
        }
        return sendClientRequest(requestBuilder, actor).getEntity();
    }
}
