package com.linkedin.metadata.search.elasticsearch.index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.SemanticSourceToken;
import com.linkedin.metadata.search.elasticsearch.index.SemanticContentIndexFields.DocumentValues;
import com.linkedin.metadata.search.elasticsearch.index.SemanticContentIndexFields.ProfileValues;
import com.linkedin.metadata.search.elasticsearch.index.SemanticContentIndexFields.Searcher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SemanticContentIndexFieldsTest {

  private Searcher searcher;
  private OperationContext opContext;
  private SemanticContentIndexFields fields;

  @BeforeMethod
  public void setup() {
    searcher = mock(Searcher.class);
    opContext = mock(OperationContext.class);
    fields = new SemanticContentIndexFields(searcher);
  }

  @Test
  public void testEmptyDocumentIdsSkipsSearch() throws Exception {
    Map<String, DocumentValues> result =
        fields.byDocumentId(opContext, "documentindex_v3", List.of(), List.of("cohere_v3"));

    assertTrue(result.isEmpty());
    verifyNoInteractions(searcher);
  }

  @Test
  public void testByDocumentIdFetchesMinimalKeyFieldsWithoutChunksAndParsesValues()
      throws Exception {
    Map<String, Object> rootMarker =
        Map.of("sources", Map.of("documentInfo", Map.of("createdTime", 1000L, "version", 2L)));
    Map<String, Object> profileMarker =
        Map.of(
            "sources",
            Map.of("documentInfo#contents.text", Map.of("createdTime", 900, "version", 1)));

    Map<String, Object> activeProfile = new HashMap<>();
    activeProfile.put("vectorState", "READY");
    activeProfile.put("current", true);
    activeProfile.put("contentHash", "hash-1");
    activeProfile.put("sourceTextSha256", "sha-1");
    activeProfile.put("totalChunks", 3);
    activeProfile.put("generatedAt", 1700000000000L);
    activeProfile.put("refreshMarker", profileMarker);

    Map<String, Object> embeddings = new HashMap<>();
    embeddings.put("cohere_v3", activeProfile);
    embeddings.put("null_model", null);
    embeddings.put("retired_model", Map.of("totalChunks", 1, "generatedAt", 500L));

    Map<String, Object> source1 = new HashMap<>();
    source1.put("urn", "urn:li:document:doc-1");
    source1.put("skipReason", null);
    source1.put("refreshMarker", rootMarker);
    source1.put("embeddings", embeddings);

    SearchHit hit1 = mock(SearchHit.class);
    when(hit1.getId()).thenReturn("id-1");
    when(hit1.getSourceAsMap()).thenReturn(source1);

    SearchHit hit2 = mock(SearchHit.class);
    when(hit2.getId()).thenReturn("id-2");
    when(hit2.getSourceAsMap())
        .thenReturn(Map.of("urn", "urn:li:document:doc-2", MappingConstants.TOMBSTONE_FIELD, true));

    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits =
        new SearchHits(
            new SearchHit[] {hit1, hit2}, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1.0f);
    when(response.getHits()).thenReturn(hits);
    when(searcher.search(eq(opContext), eq("documentindex_v3"), any())).thenReturn(response);

    Map<String, DocumentValues> result =
        fields.byDocumentId(
            opContext,
            "documentindex_v3",
            List.of("id-1", "id-2", "id-missing"),
            List.of("cohere_v3", "null_model"));

    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(searcher).search(eq(opContext), eq("documentindex_v3"), requestCaptor.capture());
    FetchSourceContext fetchSource = requestCaptor.getValue().source().fetchSource();
    List<String> includes = Arrays.asList(fetchSource.includes());
    assertTrue(includes.contains("urn"));
    assertTrue(includes.contains(MappingConstants.TOMBSTONE_FIELD));
    assertTrue(includes.contains("skipReason"));
    assertTrue(includes.contains("refreshMarker"));
    assertTrue(includes.contains("embeddings.cohere_v3.vectorState"));
    assertTrue(includes.contains("embeddings.cohere_v3.totalChunks"));
    assertFalse(includes.stream().anyMatch(path -> path.contains("chunks")));
    assertFalse(
        includes.stream()
            .anyMatch(path -> path.contains("vector") && !path.endsWith("vectorState")));
    assertFalse(includes.stream().anyMatch(path -> path.contains("retired_model")));

    assertEquals(result.size(), 2);
    DocumentValues doc1 = result.get("id-1");
    assertNotNull(doc1);
    assertEquals(doc1.urn(), "urn:li:document:doc-1");
    assertFalse(doc1.tombstone());
    assertNull(doc1.skipReason());
    assertNotNull(doc1.refreshMarker());
    assertEquals(
        doc1.refreshMarker().getSources().get("documentInfo"),
        new SemanticSourceToken().setCreatedTime(1000L).setVersion(2L));
    assertEquals(doc1.profiles().size(), 1);
    assertFalse(doc1.profiles().containsKey("null_model"));
    assertFalse(doc1.profiles().containsKey("retired_model"));

    ProfileValues profile = doc1.profiles().get("cohere_v3");
    assertEquals(profile.vectorState(), "READY");
    assertEquals(profile.current(), Boolean.TRUE);
    assertEquals(profile.contentHash(), "hash-1");
    assertEquals(profile.sourceTextSha256(), "sha-1");
    assertEquals(profile.totalChunks(), Integer.valueOf(3));
    assertEquals(profile.generatedAt(), Long.valueOf(1700000000000L));
    assertNotNull(profile.refreshMarker());
    assertEquals(
        profile.refreshMarker().getSources().get("documentInfo#contents.text"),
        new SemanticSourceToken().setCreatedTime(900L).setVersion(1L));

    DocumentValues doc2 = result.get("id-2");
    assertNotNull(doc2);
    assertTrue(doc2.tombstone());
  }
}
