/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.graph.elastic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import java.util.Optional;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.script.Script;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ESGraphWriteDAOTest {
  public static final IndexConvention TEST_INDEX_CONVENTION =
      IndexConventionImpl.noPrefix("md5", SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);

  private ESBulkProcessor mockBulkProcessor;
  private GraphQueryConfiguration config;
  private ESGraphWriteDAO testDao;
  private OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @BeforeMethod
  public void setupTest() {
    mockBulkProcessor = mock(ESBulkProcessor.class);
    config = GraphQueryConfiguration.builder().graphStatusEnabled(true).build();
    testDao = new ESGraphWriteDAO(TEST_INDEX_CONVENTION, mockBulkProcessor, 0, config);
  }

  @DataProvider(name = "writabilityConfig")
  public Object[][] writabilityConfigProvider() {
    return new Object[][] {
      {true, "Writable"}, // canWrite = true, description
      {false, "ReadOnly"} // canWrite = false, description
    };
  }

  @Test
  public void testUpdateByQuery() {
    ESBulkProcessor mockBulkProcess = mock(ESBulkProcessor.class);
    GraphQueryConfiguration config =
        GraphQueryConfiguration.builder().graphStatusEnabled(true).build();
    ESGraphWriteDAO test = new ESGraphWriteDAO(TEST_INDEX_CONVENTION, mockBulkProcess, 0, config);

    test.updateByQuery(new Script("test"), QueryBuilders.boolQuery());

    verify(mockBulkProcess)
        .updateByQuery(
            eq(new Script("test")), eq(QueryBuilders.boolQuery()), eq("graph_service_v1"));
    verifyNoMoreInteractions(mockBulkProcess);
  }

  @Test(dataProvider = "writabilityConfig")
  public void testUpsertDocumentWithWritability(boolean canWrite, String description) {
    // Set writability
    testDao.setWritable(canWrite);

    String docId = "testDoc" + description;
    String document = "{\"test\": \"data\"}";

    // Call upsertDocument
    testDao.upsertDocument(docId, document);

    if (canWrite) {
      // When writable, bulkProcessor.add should be called
      verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));
    } else {
      // When not writable, bulkProcessor.add should not be called
      verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteDocumentWithWritability(boolean canWrite, String description) {
    // Set writability
    testDao.setWritable(canWrite);

    String docId = "testDoc" + description;

    // Call deleteDocument
    testDao.deleteDocument(docId);

    if (canWrite) {
      // When writable, bulkProcessor.add should be called with DeleteRequest
      verify(mockBulkProcessor, times(1)).add(any(DeleteRequest.class));
    } else {
      // When not writable, bulkProcessor.add should not be called
      verify(mockBulkProcessor, never()).add(any(DeleteRequest.class));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteByQueryWithWritability(boolean canWrite, String description) {
    testDao.setWritable(canWrite);

    GraphFilters mockFilters = mock(GraphFilters.class);
    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);

    when(mockBulkProcessor.deleteByQuery(any(), any())).thenReturn(Optional.of(mockResponse));

    BulkByScrollResponse result = testDao.deleteByQuery(opContext, mockFilters);

    if (canWrite) {
      // When writable, should call bulkProcessor.deleteByQuery and return response
      verify(mockBulkProcessor, times(1)).deleteByQuery(any(), any());
      assertNotNull(result, "Should return response when writable");
    } else {
      // When not writable, should not call bulkProcessor and return null
      verify(mockBulkProcessor, never()).deleteByQuery(any(), any());
      assertNull(result, "Should return null when not writable");
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteByQueryWithLifecycleOwnerWritability(boolean canWrite, String description) {
    testDao.setWritable(canWrite);

    GraphFilters mockFilters = mock(GraphFilters.class);
    String lifecycleOwner = "testOwner";
    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);

    when(mockBulkProcessor.deleteByQuery(any(), any())).thenReturn(Optional.of(mockResponse));

    BulkByScrollResponse result = testDao.deleteByQuery(opContext, mockFilters, lifecycleOwner);

    if (canWrite) {
      // When writable, should call bulkProcessor.deleteByQuery and return response
      verify(mockBulkProcessor, times(1)).deleteByQuery(any(), any());
      assertNotNull(result, "Should return response when writable");
    } else {
      // When not writable, should not call bulkProcessor and return null
      verify(mockBulkProcessor, never()).deleteByQuery(any(), any());
      assertNull(result, "Should return null when not writable");
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testUpdateByQueryWithWritability(boolean canWrite, String description) {
    testDao.setWritable(canWrite);

    Script script = new Script("test script");
    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);

    when(mockBulkProcessor.updateByQuery(any(), any(), any()))
        .thenReturn(Optional.of(mockResponse));

    BulkByScrollResponse result = testDao.updateByQuery(script, QueryBuilders.boolQuery());

    if (canWrite) {
      // When writable, should call bulkProcessor.updateByQuery and return response
      verify(mockBulkProcessor, times(1)).updateByQuery(any(), any(), any());
      assertNotNull(result, "Should return response when writable");
    } else {
      // When not writable, should not call bulkProcessor and return null
      verify(mockBulkProcessor, never()).updateByQuery(any(), any(), any());
      assertNull(result, "Should return null when not writable");
    }
  }

  @Test
  public void testSetWritableToggle() {
    testDao.setWritable(true);

    String docId1 = "doc1";
    String document1 = "{\"test\": \"data1\"}";

    // Upsert should work when writable
    testDao.upsertDocument(docId1, document1);
    verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));

    testDao.setWritable(false);

    String docId2 = "doc2";
    String document2 = "{\"test\": \"data2\"}";

    testDao.upsertDocument(docId2, document2);

    // Should still only have 1 call from before
    verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));

    testDao.setWritable(true);

    String docId3 = "doc3";
    String document3 = "{\"test\": \"data3\"}";

    // Upsert should work again
    testDao.upsertDocument(docId3, document3);
    verify(mockBulkProcessor, times(2)).add(any(UpdateRequest.class));
  }

  @Test
  public void testAllWriteOperationsBlockedWhenNotWritable() {
    testDao.setWritable(false);

    testDao.upsertDocument("doc1", "{\"test\": \"data\"}");
    verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));

    testDao.deleteDocument("doc2");
    verify(mockBulkProcessor, never()).add(any(DeleteRequest.class));

    GraphFilters mockFilters = mock(GraphFilters.class);
    BulkByScrollResponse deleteResult = testDao.deleteByQuery(opContext, mockFilters);
    assertNull(deleteResult, "deleteByQuery should return null when not writable");
    verify(mockBulkProcessor, never()).deleteByQuery(any(), any());

    BulkByScrollResponse deleteWithOwnerResult =
        testDao.deleteByQuery(opContext, mockFilters, "owner");
    assertNull(deleteWithOwnerResult, "deleteByQuery should return null when not writable");

    BulkByScrollResponse updateResult =
        testDao.updateByQuery(new Script("test"), QueryBuilders.boolQuery());
    assertNull(updateResult, "updateByQuery should return null when not writable");
    verify(mockBulkProcessor, never()).updateByQuery(any(), any(), any());

    verifyNoMoreInteractions(mockBulkProcessor);
  }

  @Test
  public void testWritabilityDuringMigration() {
    testDao.setWritable(false);

    // All write operations should be blocked
    testDao.upsertDocument("migrationDoc", "{\"test\": \"data\"}");
    testDao.deleteDocument("migrationDoc");

    GraphFilters mockFilters = mock(GraphFilters.class);
    BulkByScrollResponse deleteResult = testDao.deleteByQuery(opContext, mockFilters);
    BulkByScrollResponse deleteWithOwnerResult =
        testDao.deleteByQuery(opContext, mockFilters, "owner");
    BulkByScrollResponse updateResult =
        testDao.updateByQuery(new Script("test"), QueryBuilders.boolQuery());

    // All should return null or not execute
    assertNull(deleteResult, "Delete blocked during migration");
    assertNull(deleteWithOwnerResult, "Delete with owner blocked during migration");
    assertNull(updateResult, "Update blocked during migration");

    verifyNoMoreInteractions(mockBulkProcessor);

    testDao.setWritable(true);

    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);
    when(mockBulkProcessor.updateByQuery(any(), any(), any()))
        .thenReturn(Optional.of(mockResponse));

    // Writes should work again
    BulkByScrollResponse postMigrationResult =
        testDao.updateByQuery(new Script("post-migration"), QueryBuilders.boolQuery());
    assertNotNull(postMigrationResult, "Writes work after migration");
    verify(mockBulkProcessor, times(1)).updateByQuery(any(), any(), any());
  }

  @Test
  public void testMultipleOperationsInSequence() {
    testDao.setWritable(true);

    testDao.upsertDocument("doc1", "{\"seq\": 1}");
    testDao.upsertDocument("doc2", "{\"seq\": 2}");
    testDao.deleteDocument("doc3");

    verify(mockBulkProcessor, times(2)).add(any(UpdateRequest.class));
    verify(mockBulkProcessor, times(1)).add(any(DeleteRequest.class));

    testDao.setWritable(false);

    testDao.upsertDocument("doc4", "{\"seq\": 4}");
    testDao.deleteDocument("doc5");

    // Counts should not increase
    verify(mockBulkProcessor, times(2)).add(any(UpdateRequest.class));
    verify(mockBulkProcessor, times(1)).add(any(DeleteRequest.class));

    testDao.setWritable(true);

    // Operations should work again
    testDao.upsertDocument("doc6", "{\"seq\": 6}");
    verify(mockBulkProcessor, times(3)).add(any(UpdateRequest.class));
  }

  @Test
  public void testUpsertDocumentParameters() {
    testDao.setWritable(true);

    String docId = "testDocId";
    String document = "{\"field\": \"value\"}";

    testDao.upsertDocument(docId, document);

    // Verify the UpdateRequest was created with correct parameters
    verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));
  }

  @Test
  public void testDeleteDocumentParameters() {
    testDao.setWritable(true);

    String docId = "testDeleteDocId";

    testDao.deleteDocument(docId);

    // Verify the DeleteRequest was created with correct parameters
    verify(mockBulkProcessor, times(1)).add(any(DeleteRequest.class));
  }

  @Test
  public void testDeleteByQueryReturnsBulkResponse() {
    testDao.setWritable(true);

    GraphFilters mockFilters = mock(GraphFilters.class);
    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);

    when(mockBulkProcessor.deleteByQuery(any(), any())).thenReturn(Optional.of(mockResponse));

    BulkByScrollResponse result = testDao.deleteByQuery(opContext, mockFilters);

    assertNotNull(result, "Should return BulkByScrollResponse when writable");
    verify(mockBulkProcessor, times(1)).deleteByQuery(any(), any());
  }

  @Test
  public void testDeleteByQueryReturnsNullWhenEmpty() {
    testDao.setWritable(true);

    GraphFilters mockFilters = mock(GraphFilters.class);

    // Return empty Optional
    when(mockBulkProcessor.deleteByQuery(any(), any())).thenReturn(Optional.empty());

    BulkByScrollResponse result = testDao.deleteByQuery(opContext, mockFilters);

    assertNull(result, "Should return null when bulkProcessor returns empty Optional");
    verify(mockBulkProcessor, times(1)).deleteByQuery(any(), any());
  }

  @Test
  public void testUpdateByQueryReturnsNullWhenEmpty() {
    testDao.setWritable(true);

    Script script = new Script("test");

    // Return empty Optional
    when(mockBulkProcessor.updateByQuery(any(), any(), any())).thenReturn(Optional.empty());

    BulkByScrollResponse result = testDao.updateByQuery(script, QueryBuilders.boolQuery());

    assertNull(result, "Should return null when bulkProcessor returns empty Optional");
    verify(mockBulkProcessor, times(1)).updateByQuery(any(), any(), any());
  }
}
