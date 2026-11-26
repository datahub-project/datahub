package com.linkedin.metadata.elasticsearch.update;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.update.DocWriteRequests;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.VersionType;
import org.testng.annotations.Test;

public class DocWriteRequestsTest {

  @Test
  public void testIndexRequestIsCopiedOntoTheNewIndexWithItsContent() {
    IndexRequest original =
        new IndexRequest("src_idx")
            .id("doc-1")
            .source("{\"a\":1}", XContentType.JSON)
            .opType(DocWriteRequest.OpType.CREATE)
            .versionType(VersionType.EXTERNAL)
            .version(42L);

    IndexRequest copy = (IndexRequest) DocWriteRequests.withIndex(original, "dst_idx");

    assertNotSame(copy, original);
    assertEquals(original.index(), "src_idx");
    assertEquals(copy.index(), "dst_idx");
    assertEquals(copy.id(), "doc-1");
    assertEquals(copy.source().utf8ToString(), "{\"a\":1}");
    assertEquals(copy.opType(), DocWriteRequest.OpType.CREATE);
    assertEquals(copy.versionType(), VersionType.EXTERNAL);
    assertEquals(copy.version(), 42L);
  }

  @Test
  public void testUpdateRequestKeepsUpsertSemantics() {
    UpdateRequest original =
        new UpdateRequest("src_idx", "doc-2")
            .docAsUpsert(true)
            .detectNoop(false)
            .retryOnConflict(3)
            .doc("{\"b\":2}", XContentType.JSON);

    UpdateRequest copy = (UpdateRequest) DocWriteRequests.withIndex(original, "dst_idx");

    assertNotSame(copy, original);
    assertEquals(copy.index(), "dst_idx");
    assertEquals(copy.id(), "doc-2");
    assertTrue(copy.docAsUpsert());
    assertEquals(copy.detectNoop(), false);
    assertEquals(copy.retryOnConflict(), 3);
    assertEquals(copy.doc().source().utf8ToString(), "{\"b\":2}");
  }

  @Test
  public void testDeleteRequestIsCopiedOntoTheNewIndex() {
    DeleteRequest copy =
        (DeleteRequest)
            DocWriteRequests.withIndex(new DeleteRequest("src_idx", "doc-3"), "dst_idx");

    assertEquals(copy.index(), "dst_idx");
    assertEquals(copy.id(), "doc-3");
  }

  @Test
  public void testUnknownRequestTypeIsRejected() {
    DocWriteRequest<?> unknown = org.mockito.Mockito.mock(DocWriteRequest.class);
    assertThrows(IllegalArgumentException.class, () -> DocWriteRequests.withIndex(unknown, "x"));
  }
}
