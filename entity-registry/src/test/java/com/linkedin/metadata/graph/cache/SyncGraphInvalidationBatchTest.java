package com.linkedin.metadata.graph.cache;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class SyncGraphInvalidationBatchTest {

  @Test
  public void emptyBatchIsEmpty() {
    assertTrue(SyncGraphInvalidationBatch.empty().isEmpty());
  }

  @Test
  public void batchWithCreateIsNotEmpty() {
    SyncGraphInvalidationBatch batch =
        SyncGraphInvalidationBatch.builder()
            .create(
                SyncGraphInvalidationEntry.builder()
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .entityUrn("urn:li:domain:test")
                    .build())
            .build();

    assertFalse(batch.isEmpty());
  }
}
