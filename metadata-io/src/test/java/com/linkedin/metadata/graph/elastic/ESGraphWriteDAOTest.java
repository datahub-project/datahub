package com.linkedin.metadata.graph.elastic;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import org.testng.annotations.Test;

public class ESGraphWriteDAOTest {
  public static final IndexConvention TEST_INDEX_CONVENTION = IndexConventionImpl.noPrefix("md5");

  @Test
  public void testUpdateByQuery() {
    ESBulkProcessor mockBulkProcess = mock(ESBulkProcessor.class);
    GraphQueryConfiguration config = new GraphQueryConfiguration();
    config.setGraphStatusEnabled(true);
    ESGraphWriteDAO test = new ESGraphWriteDAO(TEST_INDEX_CONVENTION, mockBulkProcess, 0, config);

    test.updateByQuery(new Script("test"), QueryBuilders.boolQuery());

    verify(mockBulkProcess)
        .updateByQuery(
            eq(new Script("test")), eq(QueryBuilders.boolQuery()), eq("graph_service_v1"));
    verifyNoMoreInteractions(mockBulkProcess);
  }
}
