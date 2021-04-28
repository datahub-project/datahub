package com.linkedin.datahub.graphql;

import com.linkedin.chart.client.Charts;
import com.linkedin.dashboard.client.Dashboards;
import com.linkedin.dataplatform.client.DataPlatforms;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.identity.client.CorpUsers;
import com.linkedin.lineage.client.Lineages;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.ml.client.MLModels;
import com.linkedin.restli.client.Client;
import com.linkedin.tag.client.Tags;
import com.linkedin.util.Configuration;
import com.linkedin.datajob.client.DataFlows;
import com.linkedin.datajob.client.DataJobs;


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

    private static CorpUsers _corpUsers;
    private static Datasets _datasets;
    private static Dashboards _dashboards;
    private static Charts _charts;
    private static DataPlatforms _dataPlatforms;
    private static MLModels _mlModels;
    private static Lineages _lineages;
    private static Tags _tags;
    private static DataFlows _dataFlows;
    private static DataJobs _dataJobs;


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

    public static Dashboards getDashboardsClient() {
        if (_dashboards == null) {
            synchronized (GmsClientFactory.class) {
                if (_dashboards == null) {
                    _dashboards = new Dashboards(REST_CLIENT);
                }
            }
        }
        return _dashboards;
    }

    public static Charts getChartsClient() {
        if (_charts == null) {
            synchronized (GmsClientFactory.class) {
                if (_charts == null) {
                    _charts = new Charts(REST_CLIENT);
                }
            }
        }
        return _charts;
    }

    public static DataPlatforms getDataPlatformsClient() {
        if (_dataPlatforms == null) {
            synchronized (GmsClientFactory.class) {
                if (_dataPlatforms == null) {
                    _dataPlatforms = new DataPlatforms(REST_CLIENT);
                }
            }
        }
        return _dataPlatforms;
    }

    public static MLModels getMLModelsClient() {
        if (_mlModels == null) {
            synchronized (GmsClientFactory.class) {
                if (_mlModels == null) {
                    _mlModels = new MLModels(REST_CLIENT);
                }
            }
        }
        return _mlModels;
    }

    public static DataFlows getDataFlowsClient() {
        if (_dataFlows == null) {
            synchronized (GmsClientFactory.class) {
                if (_dataFlows == null) {
                    _dataFlows = new DataFlows(REST_CLIENT);
                }
            }
        }
        return _dataFlows;
    }

    public static DataJobs getDataJobsClient() {
        if (_dataJobs == null) {
            synchronized (GmsClientFactory.class) {
                if (_dataJobs == null) {
                    _dataJobs = new DataJobs(REST_CLIENT);
                }
            }
        }
        return _dataJobs;
    }

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

    public static Tags getTagsClient() {
        if (_tags == null) {
            synchronized (GmsClientFactory.class) {
                if (_tags == null) {
                    _tags = new Tags(REST_CLIENT);
                }
            }
        }
        return _tags;
    }
}
