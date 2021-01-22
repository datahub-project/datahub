package com.linkedin.datahub.graphql.loaders;

import com.linkedin.dataset.client.Datasets;
import com.linkedin.identity.client.CorpUsers;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.restli.client.Client;
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

    private static final Client REST_CLIENT = DefaultRestliClientFactory.getRestLiClient(
            Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR),
            Integer.valueOf(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR)));

    private static CorpUsers _corpUsers;
    private static Datasets _datasets;

    private GmsClientFactory() { }

    public static CorpUsers getCorpUsersClient() {
        if (_corpUsers == null) {
            synchronized (GmsClientFactory.class) {
                if (_corpUsers == null) {
                    _corpUsers = new CorpUsers(REST_CLIENT);
                }
            }
        }
        return _corpUsers;
    }

    public static Datasets getDatasetsClient() {
        if (_datasets == null) {
            synchronized (GmsClientFactory.class) {
                if (_datasets == null) {
                    _datasets = new Datasets(REST_CLIENT);
                }
            }
        }
        return _datasets;
    }
}
