package com.linkedin.metadata.elasticsearch.update;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.update.BulkItemFailureClassifier;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.Test;

public class BulkItemFailureClassifierTest {

  @Test
  public void testVersionConflict() {
    assertTrue(BulkItemFailureClassifier.isVersionConflict("version_conflict_engine_exception"));
    assertTrue(BulkItemFailureClassifier.isVersionConflict("Version_Conflict elsewhere"));
    assertFalse(BulkItemFailureClassifier.isVersionConflict("mapper_parsing_exception"));
    assertFalse(BulkItemFailureClassifier.isVersionConflict(null));
  }

  @Test
  public void testDocumentMissing() {
    assertTrue(BulkItemFailureClassifier.isDocumentMissing("document_missing_exception"));
    assertFalse(BulkItemFailureClassifier.isDocumentMissing("version_conflict_engine_exception"));
  }

  @Test
  public void testRetriable() {
    assertTrue(
        BulkItemFailureClassifier.isRetriableFailure(
            RestStatus.CONFLICT, "version_conflict_engine_exception"));
    assertTrue(
        BulkItemFailureClassifier.isRetriableFailure(RestStatus.TOO_MANY_REQUESTS, "rejected"));
    assertTrue(
        BulkItemFailureClassifier.isRetriableFailure(
            RestStatus.SERVICE_UNAVAILABLE, "unavailable"));
    assertTrue(BulkItemFailureClassifier.isRetriableFailure(null, "rejected_execution_exception"));
    assertFalse(
        BulkItemFailureClassifier.isRetriableFailure(
            RestStatus.BAD_REQUEST, "document_missing_exception"));
    assertFalse(
        BulkItemFailureClassifier.isRetriableFailure(RestStatus.BAD_REQUEST, "mapper_parsing"));
  }
}
