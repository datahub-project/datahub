package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import org.testng.annotations.Test;

public class V3SearchDocumentWriteTest {

  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,my_db.my_table,PROD)");

  @Test
  public void testFillCopyOfAMergeOnlyUpdatesAnExistingDocument() {
    V3SearchDocumentWrite merge = write(V3SearchDocumentWrite.Kind.MERGE);

    V3SearchDocumentWrite copy = merge.redirectedToFill("test_groupindex_v3_next");

    assertEquals(copy.indexName(), "test_groupindex_v3_next");
    assertEquals(copy.kind(), V3SearchDocumentWrite.Kind.MERGE_EXISTING);
  }

  @Test
  public void testFillCopyOfATombstoneStaysUnconditional() {
    V3SearchDocumentWrite tombstone = write(V3SearchDocumentWrite.Kind.TOMBSTONE);

    assertEquals(
        tombstone.redirectedToFill("test_groupindex_v3_next").kind(),
        V3SearchDocumentWrite.Kind.TOMBSTONE);
  }

  @Test
  public void testFillCopyOfADeleteIsATombstone() {
    // The fill creates only absent documents, so a removed one in its target would let it bring
    // the entity back.
    V3SearchDocumentWrite delete = write(V3SearchDocumentWrite.Kind.DELETE);

    V3SearchDocumentWrite copy = delete.redirectedToFill("test_groupindex_v3_next");

    assertEquals(copy.kind(), V3SearchDocumentWrite.Kind.TOMBSTONE);
    assertEquals(copy.source(), delete.source());
  }

  @Test
  public void testRollbackCopyOfADeleteStaysADelete() {
    assertEquals(
        write(V3SearchDocumentWrite.Kind.DELETE).redirectedTo("test_groupindex_v3_old").kind(),
        V3SearchDocumentWrite.Kind.DELETE);
  }

  private static V3SearchDocumentWrite write(V3SearchDocumentWrite.Kind kind) {
    return new V3SearchDocumentWrite(
        "test_groupindex_v3", "docId", URN, "{\"urn\":\"" + URN + "\"}", kind);
  }
}
