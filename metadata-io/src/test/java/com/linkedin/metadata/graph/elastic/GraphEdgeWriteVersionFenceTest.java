package com.linkedin.metadata.graph.elastic;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphEdgeWriteVersionFenceTest {

  @BeforeMethod
  public void setUp() {
    GraphEdgeWriteVersionFence.INSTANCE.resetForTesting();
  }

  @Test
  public void testDeclineStaleUpsertRequeueAfterNewerDelete() {
    synchronized (GraphEdgeWriteVersionFence.class) {
      GraphEdgeWriteVersionFence.INSTANCE.resetForTesting();
      String docId = "edge-doc-1";
      UpdateRequest upsertV2 = new UpdateRequest("graph_service_v1", docId);
      DeleteRequest deleteV3 = new DeleteRequest("graph_service_v1").id(docId);

      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 2L, upsertV2);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 3L, deleteV3);

      assertTrue(GraphEdgeWriteVersionFence.INSTANCE.shouldDeclineRequeue(upsertV2));
      assertFalse(GraphEdgeWriteVersionFence.INSTANCE.shouldDeclineRequeue(deleteV3));
    }
  }

  @Test
  public void testDeclineStaleDeleteRequeueAfterNewerUpsert() {
    synchronized (GraphEdgeWriteVersionFence.class) {
      GraphEdgeWriteVersionFence.INSTANCE.resetForTesting();
      String docId = "edge-doc-2";
      DeleteRequest deleteV2 = new DeleteRequest("graph_service_v1").id(docId);
      UpdateRequest upsertV3 = new UpdateRequest("graph_service_v1", docId);

      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 2L, deleteV2);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 3L, upsertV3);

      assertTrue(GraphEdgeWriteVersionFence.INSTANCE.shouldDeclineRequeue(deleteV2));
      assertFalse(GraphEdgeWriteVersionFence.INSTANCE.shouldDeclineRequeue(upsertV3));
    }
  }

  @Test
  public void testUnversionedRequestsNeverDeclined() {
    synchronized (GraphEdgeWriteVersionFence.class) {
      GraphEdgeWriteVersionFence.INSTANCE.resetForTesting();
      String docId = "edge-doc-3";
      UpdateRequest upsert = new UpdateRequest("graph_service_v1", docId);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, null, upsert);
      assertFalse(GraphEdgeWriteVersionFence.INSTANCE.shouldDeclineRequeue(upsert));
    }
  }

  @Test
  public void testEqualVersionAllowedToRequeue() {
    synchronized (GraphEdgeWriteVersionFence.class) {
      GraphEdgeWriteVersionFence.INSTANCE.resetForTesting();
      String docId = "edge-doc-4";
      UpdateRequest first = new UpdateRequest("graph_service_v1", docId);
      UpdateRequest second = new UpdateRequest("graph_service_v1", docId);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 5L, first);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 5L, second);
      assertFalse(GraphEdgeWriteVersionFence.INSTANCE.shouldDeclineRequeue(first));
    }
  }
}
