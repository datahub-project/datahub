package com.linkedin.metadata.entity.hooks.buffer;

import com.linkedin.metadata.aspect.batch.MCPItem;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Sink for the MCPs a deferred post-commit hook generates during background replay. The {@link
 * PostCommitHookDrainer} calls this with the hook's output; the wired implementation (in {@code
 * metadata-service}) feeds them back through the normal async ingest path, exactly as the
 * synchronous {@code processPostCommitMCLSideEffects} path does inline. Keeping this behind a
 * functional interface lets the drainer (in {@code metadata-io}) stay free of the {@code
 * EntityServiceImpl} ingest surface and be unit-tested with a fake sink.
 */
@FunctionalInterface
public interface PostCommitHookSink {
  void emit(@Nonnull OperationContext opContext, @Nonnull List<MCPItem> mcps);
}
