package com.linkedin.metadata.graph.elastic;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.script.Script;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ElasticSearchGraphServiceTest {

  private ElasticSearchGraphService test;
  private ESBulkProcessor mockESBulkProcessor;
  private ESGraphWriteDAO mockWriteDAO;
  private ESGraphQueryDAO mockReadDAO;

  @BeforeTest
  public void beforeTest() {
    EntityRegistry entityRegistry = new TestEntityRegistry();
    mockESBulkProcessor = mock(ESBulkProcessor.class);
    mockWriteDAO = mock(ESGraphWriteDAO.class);
    mockReadDAO = mock(ESGraphQueryDAO.class);
    test =
        new ElasticSearchGraphService(
            new LineageRegistry(entityRegistry),
            mockESBulkProcessor,
            IndexConventionImpl.noPrefix("md5"),
            mockWriteDAO,
            mockReadDAO,
            mock(ESIndexBuilder.class),
            "md5");
  }

  @BeforeMethod
  public void beforeMethod() {
    reset(mockESBulkProcessor, mockWriteDAO, mockReadDAO);
  }

  @Test
  public void testSetEdgeStatus() {
    final Urn testUrn = UrnUtils.getUrn("urn:li:container:test");
    for (boolean removed : Set.of(true, false)) {
      test.setEdgeStatus(testUrn, removed, EdgeUrnType.values());

      ArgumentCaptor<Script> scriptCaptor = ArgumentCaptor.forClass(Script.class);
      ArgumentCaptor<QueryBuilder> queryCaptor = ArgumentCaptor.forClass(QueryBuilder.class);
      verify(mockWriteDAO, times(EdgeUrnType.values().length))
          .updateByQuery(scriptCaptor.capture(), queryCaptor.capture());

      queryCaptor
          .getAllValues()
          .forEach(
              queryBuilder -> {
                BoolQueryBuilder query = (BoolQueryBuilder) queryBuilder;

                // urn targeted
                assertEquals(
                    ((TermQueryBuilder) query.filter().get(0)).value(), testUrn.toString());

                // Expected inverse query
                if (removed) {
                  assertEquals(((TermQueryBuilder) query.should().get(0)).value(), "false");
                  assertTrue(
                      ((ExistsQueryBuilder)
                              ((BoolQueryBuilder) query.should().get(1)).mustNot().get(0))
                          .fieldName()
                          .toLowerCase()
                          .contains("removed"));
                } else {
                  assertEquals(((TermQueryBuilder) query.filter().get(1)).value(), "true");
                }
              });

      // reset for next boolean
      reset(mockWriteDAO);
    }
  }
}
