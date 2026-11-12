package com.linkedin.datahub.graphql.types.metric.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class MetricUpstreamsMapperTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders,PROD)";
  private static final String SCHEMA_FIELD_URN =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders,PROD),revenue)";

  @Test
  public void testMapNull() {
    assertNull(MetricUpstreamsMapper.map(null, null));
  }

  @Test
  public void testMapEmpty() {
    com.linkedin.metric.MetricUpstreams pdl = new com.linkedin.metric.MetricUpstreams();

    com.linkedin.datahub.graphql.generated.MetricUpstreams result =
        MetricUpstreamsMapper.map(null, pdl);

    assertNotNull(result);
    assertNull(result.getDatasetUpstreams());
    assertNull(result.getFieldUpstreams());
  }

  @Test
  public void testMapDatasetUpstreams() throws URISyntaxException {
    Edge edge = new Edge().setDestinationUrn(Urn.createFromString(DATASET_URN));
    com.linkedin.metric.MetricUpstreams pdl = new com.linkedin.metric.MetricUpstreams();
    pdl.setDatasetUpstreams(new EdgeArray(edge));

    com.linkedin.datahub.graphql.generated.MetricUpstreams result =
        MetricUpstreamsMapper.map(null, pdl);

    assertNotNull(result);
    assertNotNull(result.getDatasetUpstreams());
    assertEquals(result.getDatasetUpstreams().size(), 1);
    assertEquals(result.getDatasetUpstreams().get(0).getDestination().getUrn(), DATASET_URN);
    assertNull(result.getFieldUpstreams());
  }

  @Test
  public void testMapFieldUpstreams() throws URISyntaxException {
    Edge edge = new Edge().setDestinationUrn(Urn.createFromString(SCHEMA_FIELD_URN));
    com.linkedin.metric.MetricUpstreams pdl = new com.linkedin.metric.MetricUpstreams();
    pdl.setFieldUpstreams(new EdgeArray(edge));

    com.linkedin.datahub.graphql.generated.MetricUpstreams result =
        MetricUpstreamsMapper.map(null, pdl);

    assertNotNull(result);
    assertNull(result.getDatasetUpstreams());
    assertNotNull(result.getFieldUpstreams());
    assertEquals(result.getFieldUpstreams().size(), 1);
    assertEquals(result.getFieldUpstreams().get(0).getDestination().getUrn(), SCHEMA_FIELD_URN);
  }

  @Test
  public void testMapBothUpstreams() throws URISyntaxException {
    Edge datasetEdge = new Edge().setDestinationUrn(Urn.createFromString(DATASET_URN));
    Edge fieldEdge = new Edge().setDestinationUrn(Urn.createFromString(SCHEMA_FIELD_URN));

    com.linkedin.metric.MetricUpstreams pdl = new com.linkedin.metric.MetricUpstreams();
    pdl.setDatasetUpstreams(new EdgeArray(datasetEdge));
    pdl.setFieldUpstreams(new EdgeArray(fieldEdge));

    com.linkedin.datahub.graphql.generated.MetricUpstreams result =
        MetricUpstreamsMapper.map(null, pdl);

    assertNotNull(result);
    assertEquals(result.getDatasetUpstreams().size(), 1);
    assertEquals(result.getFieldUpstreams().size(), 1);
  }

  @Test
  public void testMapUnresolvableEdgeIsDropped() throws URISyntaxException {
    Edge goodEdge = new Edge().setDestinationUrn(Urn.createFromString(DATASET_URN));
    Edge badEdge = new Edge().setDestinationUrn(Urn.createFromString("urn:li:unknownEntity:foo"));

    com.linkedin.metric.MetricUpstreams pdl = new com.linkedin.metric.MetricUpstreams();
    pdl.setDatasetUpstreams(new EdgeArray(goodEdge, badEdge));

    com.linkedin.datahub.graphql.generated.MetricUpstreams result =
        MetricUpstreamsMapper.map(null, pdl);

    assertNotNull(result);
    // Bad edge is silently dropped by EdgeMapper
    assertEquals(result.getDatasetUpstreams().size(), 1);
    assertEquals(result.getDatasetUpstreams().get(0).getDestination().getUrn(), DATASET_URN);
  }

  @Test
  public void testMapEmptyEdgeArraysYieldEmptyLists() {
    com.linkedin.metric.MetricUpstreams pdl = new com.linkedin.metric.MetricUpstreams();
    pdl.setDatasetUpstreams(new EdgeArray());
    pdl.setFieldUpstreams(new EdgeArray());

    com.linkedin.datahub.graphql.generated.MetricUpstreams result =
        MetricUpstreamsMapper.map(null, pdl);

    assertNotNull(result);
    assertNotNull(result.getDatasetUpstreams());
    assertTrue(result.getDatasetUpstreams().isEmpty());
    assertNotNull(result.getFieldUpstreams());
    assertTrue(result.getFieldUpstreams().isEmpty());
  }
}
