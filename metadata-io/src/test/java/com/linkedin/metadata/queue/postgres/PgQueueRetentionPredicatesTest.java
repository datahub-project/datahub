package com.linkedin.metadata.queue.postgres;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PgQueueRetentionPredicatesTest {

  @Test
  public void sequenceAnchorExclusion_referencesMaxEnqueueSeq() {
    String clause =
        PgQueueRetentionPredicates.sequenceAnchorExclusion("ms", "queue.metadata_queue_message");
    assertTrue(clause.contains("m_anchor.enqueue_seq"));
    assertTrue(clause.contains("ms.topic_id"));
    assertTrue(clause.contains("ms.partition_id"));
  }
}
