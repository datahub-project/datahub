package com.linkedin.metadata.systemmetadata.scroll;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESSystemMetadataScrollClientTest {

  private ESSystemMetadataDAO dao;
  private ESSystemMetadataScrollClient client;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    dao = mock(ESSystemMetadataDAO.class);
    client = new ESSystemMetadataScrollClient(dao);
    opContext = mock(OperationContext.class);
  }

  @Test
  public void buildQuery_entityTypePrefixAndOptionalFilters() {
    Urn urn = UrnUtils.getUrn("urn:li:chart:1");
    SystemMetadataScrollRequest req =
        SystemMetadataScrollRequest.builder()
            .entityType("chart")
            .urns(java.util.Set.of(urn))
            .aspects(List.of("ChartInfo", "Ownership"))
            .gePitEpochMs(10L)
            .lePitEpochMs(20L)
            .batchSize(10)
            .build();

    BoolQueryBuilder query = client.buildQuery(req);
    String json = query.toString();
    assertTrue(json.contains("urn:li:chart:"));
    assertTrue(json.contains("ChartInfo"));
    assertTrue(json.contains("aspectModifiedTime"));
    assertTrue(json.contains("aspectCreatedTime"));
  }

  @Test
  public void scrollUrns_nullResponseThrows() {
    when(dao.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(null);

    expectThrows(
        RuntimeException.class,
        () ->
            client.scrollUrns(
                opContext,
                SystemMetadataScrollRequest.builder().entityType("chart").batchSize(10).build()));
  }

  @Test
  public void scrollUrns_emptyHitsIsEof() {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits = mock(SearchHits.class);
    when(response.getHits()).thenReturn(hits);
    when(hits.getHits()).thenReturn(new SearchHit[0]);
    when(dao.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(response);

    SystemMetadataScrollResult result =
        client.scrollUrns(
            opContext,
            SystemMetadataScrollRequest.builder().entityType("chart").batchSize(10).build());
    assertTrue(result.getUrns().isEmpty());
    assertNull(result.getNextScrollId());
  }

  @Test
  public void extractNextScrollId_usesLastHitSortValues() {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits = mock(SearchHits.class);
    SearchHit last = mock(SearchHit.class);
    when(response.getHits()).thenReturn(hits);
    when(hits.getHits()).thenReturn(new SearchHit[] {last});
    when(last.getSortValues()).thenReturn(new Object[] {"urn:li:chart:z", "ChartInfo"});

    String scrollId = client.extractNextScrollId(response);
    assertNotNull(scrollId);
    assertTrue(scrollId.length() > 0);
  }

  @Test
  public void scrollUrns_extractsUrnsAndScrollId() {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits = mock(SearchHits.class);
    SearchHit hit = mock(SearchHit.class);
    when(response.getHits()).thenReturn(hits);
    when(hits.getHits()).thenReturn(new SearchHit[] {hit});
    when(hit.getSourceAsMap()).thenReturn(Map.of("urn", "urn:li:chart:1"));
    when(hit.getSortValues()).thenReturn(new Object[] {"urn:li:chart:1", "ChartInfo"});
    when(dao.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(response);

    SystemMetadataScrollResult result =
        client.scrollUrns(
            opContext,
            SystemMetadataScrollRequest.builder().entityType("chart").batchSize(10).build());
    assertEquals(result.getUrns().size(), 1);
    assertEquals(result.getUrns().iterator().next().toString(), "urn:li:chart:1");
    assertNotNull(result.getNextScrollId());
  }
}
