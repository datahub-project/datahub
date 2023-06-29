package com.linkedin.metadata.shared;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;

import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.elasticsearch.index.query.QueryBuilder;


public interface ElasticSearchIndexed {
    String reindexAsync(String index, @Nullable QueryBuilder filterQuery, BatchWriteOperationsOptions options)
        throws Exception;

    /**
     * The index configurations for the given service.
     * @return List of reindex configurations
     */
    List<ReindexConfig> getReindexConfigs() throws IOException;

    /**
     * Mirrors the service's functions which
     * are expected to build/reindex as needed based
     * on the reindex configurations above
     */
    void reindexAll() throws IOException;
}
