package com.linkedin.metadata.search.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.script.Script;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESWriteDAOTest {

  private static final String TEST_ENTITY = "dataset";
  private static final String TEST_DOC_ID = "testDocId";
  private static final String TEST_INDEX = "datasetindex_v2";
  private static final int NUM_RETRIES = 3;
  private static final String TEST_PATTERN = "*index_v2";

  @Mock private RestHighLevelClient mockSearchClient;

  @Mock private ESBulkProcessor mockBulkProcessor;

  @Mock private org.opensearch.client.IndicesClient mockIndicesClient;

  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  private ESWriteDAO esWriteDAO;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Setup mock indices client
    when(mockSearchClient.indices()).thenReturn(mockIndicesClient);

    esWriteDAO = new ESWriteDAO(mockSearchClient, mockBulkProcessor, NUM_RETRIES);
  }

  @Test
  public void testUpsertDocument() {
    String document = "{\"field\":\"value\"}";

    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, TEST_DOC_ID);

    ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
    verify(mockBulkProcessor).add(requestCaptor.capture());

    UpdateRequest capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.index(), TEST_INDEX);
    assertEquals(capturedRequest.id(), TEST_DOC_ID);
    assertFalse(capturedRequest.detectNoop());
    assertTrue(capturedRequest.docAsUpsert());
    assertEquals(capturedRequest.retryOnConflict(), NUM_RETRIES);

    // Verify the document content
    Map<String, Object> sourceMap = capturedRequest.doc().sourceAsMap();
    assertEquals(sourceMap.get("field"), "value");
  }

  @Test
  public void testDeleteDocument() {
    esWriteDAO.deleteDocument(opContext, TEST_ENTITY, TEST_DOC_ID);

    ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
    verify(mockBulkProcessor).add(requestCaptor.capture());

    DeleteRequest capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.index(), TEST_INDEX);
    assertEquals(capturedRequest.id(), TEST_DOC_ID);
  }

  @Test
  public void testApplyScriptUpdate() {
    String scriptContent = "ctx._source.field = 'newValue'";
    Map<String, Object> upsert = new HashMap<>();
    upsert.put("field", "initialValue");

    esWriteDAO.applyScriptUpdate(opContext, TEST_ENTITY, TEST_DOC_ID, scriptContent, upsert);

    ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
    verify(mockBulkProcessor).add(requestCaptor.capture());

    UpdateRequest capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.index(), TEST_INDEX);
    assertEquals(capturedRequest.id(), TEST_DOC_ID);
    assertFalse(capturedRequest.detectNoop());
    assertTrue(capturedRequest.scriptedUpsert());
    assertEquals(capturedRequest.retryOnConflict(), NUM_RETRIES);

    // Verify script content
    Script script = capturedRequest.script();
    assertEquals(script.getIdOrCode(), scriptContent);

    // Verify upsert content
    Map<String, Object> upsertMap = capturedRequest.upsertRequest().sourceAsMap();
    assertEquals(upsertMap.get("field"), "initialValue");
  }

  @Test
  public void testClear() throws IOException {
    String[] indices = new String[] {"index1", "index2"};
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    when(mockResponse.getIndices()).thenReturn(indices);
    when(mockSearchClient.indices().get(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    esWriteDAO.clear(opContext);

    // Verify the GetIndexRequest
    ArgumentCaptor<GetIndexRequest> indexRequestCaptor =
        ArgumentCaptor.forClass(GetIndexRequest.class);
    verify(mockSearchClient.indices())
        .get(indexRequestCaptor.capture(), eq(RequestOptions.DEFAULT));
    assertEquals(indexRequestCaptor.getValue().indices()[0], TEST_PATTERN);

    // Verify the deletion query
    ArgumentCaptor<QueryBuilder> queryCaptor = ArgumentCaptor.forClass(QueryBuilder.class);
    verify(mockBulkProcessor).deleteByQuery(queryCaptor.capture(), eq(indices));
    assertTrue(queryCaptor.getValue() instanceof org.opensearch.index.query.MatchAllQueryBuilder);
  }

  @Test
  public void testClearWithIOException() throws IOException {
    when(mockSearchClient.indices().get(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new IOException("Test exception"));

    esWriteDAO.clear(opContext);

    // Verify empty array is used when exception occurs
    verify(mockBulkProcessor).deleteByQuery(any(QueryBuilder.class), eq(new String[] {}));
  }

  @Test
  public void testUpsertDocumentWithInvalidJson() {
    String invalidJson = "{invalid:json}";

    try {
      esWriteDAO.upsertDocument(opContext, TEST_ENTITY, invalidJson, TEST_DOC_ID);
    } catch (Exception e) {
      assertTrue(e instanceof org.opensearch.core.xcontent.XContentParseException);
    }
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testUpsertDocumentWithNullDocument() {
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, null, TEST_DOC_ID);
  }
}
