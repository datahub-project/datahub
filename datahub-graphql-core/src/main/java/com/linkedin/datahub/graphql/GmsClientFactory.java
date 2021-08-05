package com.linkedin.datahub.graphql;

import com.linkedin.dataplatform.client.DataPlatforms;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.lineage.client.Lineages;
import com.linkedin.lineage.client.Relationships;
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
            Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR),
            Integer.valueOf(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR)),
            Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False")),
            Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR));

    private static DataPlatforms _dataPlatforms;
    private static Lineages _lineages;
    private static Relationships _relationships;
    private static EntityClient _entities;
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

    public static Relationships getRelationshipsClient() {
        if (_relationships == null) {
            synchronized (GmsClientFactory.class) {
                if (_relationships == null) {
                    _relationships = new Relationships(REST_CLIENT);
                }
            }
        }
        return _relationships;
    }

    public static EntityClient getEntitiesClient() {
        if (_entities == null) {
            synchronized (GmsClientFactory.class) {
                if (_entities == null) {
                    _entities = new EntityClient(REST_CLIENT);
                }
            }
        }
        return _entities;
    }

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
