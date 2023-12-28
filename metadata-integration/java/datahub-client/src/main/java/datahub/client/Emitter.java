package datahub.client;

import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.event.UpsertAspectRequest;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An interface implemented by all metadata emitters to DataHub. Typical usage: 1. Construct the
 * emitter using the native constructor or builder for the Emitter. 2. Call `emitter.emit(mcpw,
 * callback)` for each event you want to send 3. Wait for all events to be sent by inspecting the
 * futures returned by each call or using callbacks 4. Call `emitter.close()` to finalize.
 */
@ThreadSafe
public interface Emitter extends Closeable {

  /**
   * Asynchronously emit a {@link MetadataChangeProposalWrapper} event.
   *
   * @param mcpw
   * @param callback if not null, is called from the IO thread. Should be a quick operation.
   * @return a {@link Future} for callers to inspect the result of the operation or block until one
   *     is available
   * @throws IOException
   */
  Future<MetadataWriteResponse> emit(@Nonnull MetadataChangeProposalWrapper mcpw, Callback callback)
      throws IOException;

  /**
   * Asynchronously emit a {@link MetadataChangeProposalWrapper} event.
   *
   * @param mcpw
   * @return a {@link Future} for callers to inspect the result of the operation or block until one
   *     is available
   * @throws IOException
   */
  default Future<MetadataWriteResponse> emit(@Nonnull MetadataChangeProposalWrapper mcpw)
      throws IOException {
    return emit(mcpw, null);
  }

  /**
   * Asynchronously emit a {@link MetadataChangeProposal} event. Prefer using the sibling method
   * that accepts a {@link MetadataChangeProposalWrapper} event as those are friendlier to
   * construct.
   *
   * @param mcp
   * @param callback if not null, is called from the IO thread. Should be a quick operation.
   * @return a {@link Future} for callers to inspect the result of the operation or block until one
   *     is available
   * @throws IOException
   */
  Future<MetadataWriteResponse> emit(@Nonnull MetadataChangeProposal mcp, Callback callback)
      throws IOException;

  /**
   * Asynchronously emit a {@link MetadataChangeProposal} event. Prefer using the sibling method
   * that accepts a {@link MetadataChangeProposalWrapper} event as those are friendlier to
   * construct.
   *
   * @param mcp
   * @return a {@link Future} for callers to inspect the result of the operation or block until one
   *     is available
   * @throws IOException
   */
  default Future<MetadataWriteResponse> emit(@Nonnull MetadataChangeProposal mcp)
      throws IOException {
    return emit(mcp, null);
  }

  /**
   * Test that the emitter can establish a valid connection to the DataHub platform
   *
   * @return true if a valid connection can be established, false or throws one of the exceptions
   *     otherwise
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  boolean testConnection() throws IOException, ExecutionException, InterruptedException;

  /**
   * Asynchronously emit a {@link UpsertAspectRequest}.
   *
   * @param request request with with metadata aspect to upsert into DataHub
   * @return a {@link Future} for callers to inspect the result of the operation or block until one
   *     is available
   * @throws IOException
   */
  Future<MetadataWriteResponse> emit(List<UpsertAspectRequest> request, Callback callback)
      throws IOException;
}
