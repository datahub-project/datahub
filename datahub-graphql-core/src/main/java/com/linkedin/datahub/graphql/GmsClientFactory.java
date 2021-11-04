package com.linkedin.datahub.graphql;

import com.linkedin.entity.client.AspectClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.lineage.client.Lineages;
import com.linkedin.lineage.client.RelationshipClient;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.restli.client.Client;
import com.linkedin.usage.UsageClient;
import com.linkedin.util.Configuration;

/**
 * Provides access to clients for use in fetching data from downstream GMS services.
 */
public class GmsClientFactory {

    /**
     * The following environment variables are expected to be provided.
     * They are used in establishing the connection to the downstream GMS.
     * Currently, only 1 downstream GMS is supported.
     */
    private static final String GMS_HOST_ENV_VAR = "DATAHUB_GMS_HOST";
    private static final String GMS_PORT_ENV_VAR = "DATAHUB_GMS_PORT";
    private static final String GMS_USE_SSL_ENV_VAR = "DATAHUB_GMS_USE_SSL";
    private static final String GMS_SSL_PROTOCOL_VAR = "DATAHUB_GMS_SSL_PROTOCOL";

    private static final Client REST_CLIENT = DefaultRestliClientFactory.getRestLiClient(
            Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR, "localhost"),
            Integer.valueOf(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR, "8080")),
            Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False")),
            Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR));

    private static Lineages _lineages;
    private static RelationshipClient _relationshipClient;
    private static RestliEntityClient _entities;
    private static AspectClient _aspects;
    private static UsageClient _usage;


    private GmsClientFactory() { }

    public static Lineages getLineagesClient() {
        if (_lineages == null) {
            synchronized (GmsClientFactory.class) {
                if (_lineages == null) {
                    _lineages = new Lineages(REST_CLIENT);
                }
            }
        }
        return _lineages;
    }

    public static RelationshipClient getRelationshipsClient() {
        if (_relationshipClient == null) {
            synchronized (GmsClientFactory.class) {
                if (_relationshipClient == null) {
                    _relationshipClient = new RelationshipClient(REST_CLIENT);
                }
            }
        }
        return _relationshipClient;
    }

    public static RestliEntityClient getEntitiesClient() {
        if (_entities == null) {
            synchronized (GmsClientFactory.class) {
                if (_entities == null) {
                    _entities = new RestliEntityClient(REST_CLIENT);
                }
            }
        }
        return _entities;
    }

    // Deprecated- please use EntityClient from now on for all aspect related calls
    @Deprecated
    public static AspectClient getAspectsClient() {
        if (_aspects == null) {
            synchronized (GmsClientFactory.class) {
                if (_aspects == null) {
                    _aspects = new AspectClient(REST_CLIENT);
                }
            }
        }
        return _aspects;
    }

    public static UsageClient getUsageClient() {
        if (_usage == null) {
            synchronized (GmsClientFactory.class) {
                if (_usage == null) {
                    _usage = new UsageClient(REST_CLIENT);
                }
            }
        }
        return _usage;
    }
}
