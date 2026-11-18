package com.linkedin.metadata.search.elasticsearch.update;

import javax.annotation.Nonnull;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.index.VersionType;

/**
 * Copies a document write request onto another index name. The shim's bulk processor keeps a
 * reference to the original request until it flushes, so a mirrored write must be a new object,
 * never the original with its index renamed.
 */
public final class DocWriteRequests {
  private DocWriteRequests() {}

  @Nonnull
  public static DocWriteRequest<?> withIndex(
      @Nonnull final DocWriteRequest<?> request, @Nonnull final String index) {
    if (request instanceof IndexRequest) {
      return copy((IndexRequest) request, index);
    }
    if (request instanceof UpdateRequest) {
      return copy((UpdateRequest) request, index);
    }
    if (request instanceof DeleteRequest) {
      return copy((DeleteRequest) request, index);
    }
    throw new IllegalArgumentException(
        "Cannot mirror a " + request.getClass().getSimpleName() + " write request");
  }

  @Nonnull
  private static IndexRequest copy(
      @Nonnull final IndexRequest request, @Nonnull final String index) {
    final IndexRequest copy = new IndexRequest(index).id(request.id());
    if (request.source() != null) {
      copy.source(request.source(), request.getContentType());
    }
    copy.opType(request.opType());
    if (request.routing() != null) {
      copy.routing(request.routing());
    }
    if (request.versionType() != VersionType.INTERNAL) {
      copy.versionType(request.versionType()).version(request.version());
    }
    if (request.getPipeline() != null) {
      copy.setPipeline(request.getPipeline());
    }
    return copy;
  }

  @Nonnull
  private static UpdateRequest copy(
      @Nonnull final UpdateRequest request, @Nonnull final String index) {
    final UpdateRequest copy =
        new UpdateRequest(index, request.id())
            .docAsUpsert(request.docAsUpsert())
            .detectNoop(request.detectNoop())
            .scriptedUpsert(request.scriptedUpsert())
            .retryOnConflict(request.retryOnConflict());
    // Rebuild the inner documents rather than sharing them: the shim serializes the update at
    // flush time, and the (BytesReference, MediaType) overloads bind unambiguously on IndexRequest.
    if (request.doc() != null && request.doc().source() != null) {
      copy.doc(new IndexRequest().source(request.doc().source(), request.doc().getContentType()));
    }
    if (request.upsertRequest() != null && request.upsertRequest().source() != null) {
      copy.upsert(
          new IndexRequest()
              .source(request.upsertRequest().source(), request.upsertRequest().getContentType()));
    }
    if (request.script() != null) {
      copy.script(request.script());
    }
    if (request.routing() != null) {
      copy.routing(request.routing());
    }
    return copy;
  }

  @Nonnull
  private static DeleteRequest copy(
      @Nonnull final DeleteRequest request, @Nonnull final String index) {
    final DeleteRequest copy = new DeleteRequest(index, request.id());
    if (request.routing() != null) {
      copy.routing(request.routing());
    }
    return copy;
  }
}
