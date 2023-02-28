package com.linkedin.metadata.graph.elastic;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.index.query.QueryBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ESGraphQueryDAOTest {

  private static final String TEST_QUERY_FILE = "elasticsearch/sample_filters/lineage_query_filters_1.json";

  @Test
  private static void testGetQueryForLineageFullArguments() throws Exception {

    URL url = Resources.getResource(TEST_QUERY_FILE);
    String expectedQuery = Resources.toString(url, StandardCharsets.UTF_8);

    List<Urn> urns = new ArrayList<>();
    List<LineageRegistry.EdgeInfo> edgeInfos = new ArrayList<>(ImmutableList.of(
        new LineageRegistry.EdgeInfo("DownstreamOf", RelationshipDirection.INCOMING, Constants.DATASET_ENTITY_NAME)
    ));
    GraphFilters graphFilters = new GraphFilters(ImmutableList.of(Constants.DATASET_ENTITY_NAME));
    Long startTime = 0L;
    Long endTime = 1L;

    QueryBuilder builder = ESGraphQueryDAO.getQueryForLineage(
        urns,
        edgeInfos,
        graphFilters,
        startTime,
        endTime
    );

    Assert.assertEquals(builder.toString(), expectedQuery);
  }
}
