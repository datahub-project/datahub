package com.linkedin.metadata.search.elasticsearch.index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields.DocumentValues;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields.Page;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields.Searcher;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StructuredPropertyIndexFieldsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Searcher searcher;
  private OperationContext opContext;
  private StructuredPropertyIndexFields fields;

  @BeforeMethod
  public void setup() {
    searcher = mock(Searcher.class);
    opContext = mock(OperationContext.class);
    fields = new StructuredPropertyIndexFields(searcher, MAPPER);
  }

  @Test
  public void testFieldsFromSourceFlattensNestedAndDottedShapes() {
    Map<String, Object> nested = new HashMap<>();
    nested.put("kept", "yes");
    nested.put("gone", null);
    Map<String, Object> source = new HashMap<>();
    source.put("urn", "urn:li:dataset:one");
    source.put("name", "orders");
    source.put("structuredProperties", nested);
    source.put("structuredProperties.dotted", "from-root");
    source.put("structuredProperties.kept", "does-not-replace-nested");

    Map<String, com.fasterxml.jackson.databind.JsonNode> flattened =
        StructuredPropertyIndexFields.fieldsFromSource(source, MAPPER);

    assertEquals(flattened.get("structuredProperties.kept"), TextNode.valueOf("yes"));
    assertEquals(flattened.get("structuredProperties.dotted"), TextNode.valueOf("from-root"));
    assertEquals(flattened.size(), 2);
  }

  @Test
  public void testFieldsFromSourceFlattensVersionedLeaves() {
    Map<String, Object> source = new HashMap<>();
    source.put(
        "structuredProperties",
        Map.of(
            "kept",
            "yes",
            "_versioned",
            Map.of("hello", Map.of("00000000000001", Map.of("string", List.of("yes"))))));
    source.put("structuredProperties._versioned.gone.00000000000001.string", List.of("no"));

    Map<String, com.fasterxml.jackson.databind.JsonNode> flattened =
        StructuredPropertyIndexFields.fieldsFromSource(source, MAPPER);

    assertEquals(
        flattened.get("structuredProperties._versioned.hello.00000000000001.string"),
        MAPPER.valueToTree(List.of("yes")));
    assertEquals(
        flattened.get("structuredProperties._versioned.gone.00000000000001.string"),
        MAPPER.valueToTree(List.of("no")));
    assertEquals(flattened.get("structuredProperties.kept"), TextNode.valueOf("yes"));
    assertFalse(flattened.containsKey("structuredProperties._versioned"));
  }

  @Test
  public void testOneIdsQueryReadsABatchAndOmitsMissingDocuments() throws Exception {
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc-1");
    when(hit.getSourceAsMap())
        .thenReturn(
            Map.of("urn", "urn:li:dataset:one", "structuredProperties", Map.of("kept", "yes")));
    SearchResponse searchResponse = response(hit);
    when(searcher.search(any(), eq("datasetindex_v2"), any())).thenReturn(searchResponse);

    Map<String, DocumentValues> documents =
        fields.byDocumentId(opContext, "datasetindex_v2", List.of("doc-1", "doc-2"));

    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(searcher).search(any(), eq("datasetindex_v2"), request.capture());
    SearchSourceBuilder source = request.getValue().source();
    assertTrue(source.query() instanceof IdsQueryBuilder);
    assertEquals(source.size(), 2);
    assertEquals(
        List.of(source.fetchSource().includes()),
        List.of("urn", "structuredProperties", "structuredProperties.*"));
    assertEquals(documents.size(), 1);
    assertEquals(documents.get("doc-1").urn(), "urn:li:dataset:one");
    assertEquals(
        documents.get("doc-1").fields().get("structuredProperties.kept"), TextNode.valueOf("yes"));
  }

  @Test
  public void testEmptyIdsDoNotQuery() throws Exception {
    assertTrue(fields.byDocumentId(opContext, "datasetindex_v2", List.of()).isEmpty());
    verify(searcher, never()).search(any(), any(), any());
  }

  @Test
  public void testScrollRequestsOnlyDocumentsThatHaveStructuredProperties() throws Exception {
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc-1");
    when(hit.getSortValues()).thenReturn(new Object[] {"doc-1"});
    when(hit.getSourceAsMap()).thenReturn(Map.of("urn", "urn:li:dataset:one"));
    SearchResponse searchResponse = response(hit);
    when(searcher.search(any(), eq("datasetindex_v2"), any())).thenReturn(searchResponse);

    Page fullPage = fields.scroll(opContext, "datasetindex_v2", 1, null, null);

    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(searcher).search(any(), eq("datasetindex_v2"), request.capture());
    SearchSourceBuilder source = request.getValue().source();
    assertTrue(source.query() instanceof ExistsQueryBuilder);
    assertEquals(((ExistsQueryBuilder) source.query()).fieldName(), "structuredProperties");
    assertEquals(
        List.of(source.fetchSource().includes()),
        List.of("urn", "structuredProperties", "structuredProperties.*"));
    assertEquals(source.sorts().size(), 1);
    assertEquals(((FieldSortBuilder) source.sorts().get(0)).getFieldName(), "urn");
    assertEquals(fullPage.documents().size(), 1);
    assertTrue(fullPage.scrollId() != null);

    when(hit.getSortValues()).thenReturn(null);
    Page shortPage = fields.scroll(opContext, "datasetindex_v2", 2, fullPage.scrollId(), null);
    assertNull(shortPage.scrollId());
    assertTrue(shortPage.documents().get(0).fields().isEmpty());
  }

  @Test
  public void testScrollLimitsASharedIndexToTheEntityType() throws Exception {
    SearchResponse searchResponse = response();
    when(searcher.search(any(), eq("datasetindex_v3"), any())).thenReturn(searchResponse);

    Page page = fields.scroll(opContext, "datasetindex_v3", 10, null, "dataset");

    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(searcher).search(any(), eq("datasetindex_v3"), request.capture());
    BoolQueryBuilder query = (BoolQueryBuilder) request.getValue().source().query();
    assertEquals(query.filter().size(), 2);
    assertTrue(query.filter().get(0) instanceof ExistsQueryBuilder);
    assertEquals(((ExistsQueryBuilder) query.filter().get(0)).fieldName(), "structuredProperties");
    assertTrue(query.filter().get(1) instanceof TermQueryBuilder);
    TermQueryBuilder entityType = (TermQueryBuilder) query.filter().get(1);
    assertEquals(entityType.fieldName(), "_entityType");
    assertEquals(entityType.value(), "dataset");
    assertTrue(page.documents().isEmpty());
  }

  private static SearchResponse response(SearchHit... hits) {
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(
            new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f));
    return response;
  }
}
