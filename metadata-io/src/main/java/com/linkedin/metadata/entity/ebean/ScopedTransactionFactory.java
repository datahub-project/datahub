package com.linkedin.metadata.entity.ebean;

import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Transaction;
import io.ebean.TxScope;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Seam for beginning Ebean transactions and scoping non-transactional operations against an {@link
 * OperationContext}. Exists so an extension module can route each query to a different underlying
 * database without {@link EbeanAspectDao} knowing how that routing works; the OSS default is a
 * byte-identical pass-through to the single configured {@code Database}.
 */
public interface ScopedTransactionFactory {

  /**
   * Begin an explicit transaction for the given scope and operation context. Caller manages the
   * transaction lifecycle (commit / rollback / close).
   *
   * <p>Currently unused by {@code EbeanRetentionService}, which calls {@code
   * _server.beginTransaction(...)} directly for its ORM transactions (the per-context and
   * batch-scope transactions it opens do not go through this seam). Retained because cloud
   * extension modules route {@code begin} through the tenant seam (see cloud PR
   * acryldata/datahub-fork#11489); removing it here would break that routing path.
   */
  @Nonnull
  Transaction begin(@Nonnull OperationContext opContext, @Nonnull TxScope txScope);

  /**
   * Open a scope for work that does not need an explicit transaction. Any implicit transaction
   * Ebean opens while the scope is active is routed consistently with {@code opContext}. Use with
   * try-with-resources:
   *
   * <pre>{@code
   * try (Scope s = factory.scope(opContext)) {
   *   // Ebean calls here run within the operation's scope
   * }
   * }</pre>
   */
  @Nonnull
  Scope scope(@Nonnull OperationContext opContext);

  /**
   * Stack-scoped transaction scope. {@link #close()} MUST be called in LIFO order (innermost scope
   * first). Nesting is supported and expected: an inner scope shadows the outer's tenant on the
   * same thread; closing the inner scope restores the outer's tenant. All implementations MUST be
   * re-entrant: an inner scope opened before the outer closes must restore the outer's tenant on
   * close. Pop-counting / conditional unscope is NOT supported — an extension MUST NOT close a
   * scope it did not open on the same thread, and MUST NOT skip closing a scope it opened.
   * Violating LIFO order will silently unscope an outer transaction mid-batch.
   *
   * <p>Closeable scope; {@link #close()} is narrowed to declare no checked exceptions.
   */
  interface Scope extends AutoCloseable {
    /** Close this scope. Must be the most-recently-opened scope on this thread. */
    @Override
    void close();
  }

  /** Run {@code work} inside a scope and return its result. */
  default <T> T runInScope(
      @Nonnull final OperationContext opContext, @Nonnull final Supplier<T> work) {
    try (Scope s = scope(opContext)) {
      return work.get();
    }
  }

  /**
   * Run a stream-producing/consuming operation inside a scope. {@code source} and {@code consumer}
   * both run inside the scope, so implicit DB calls during stream iteration are routed
   * consistently. The stream is closed when the scope exits.
   */
  default <X, R> R inStreamScope(
      @Nonnull final OperationContext opContext,
      @Nonnull final Supplier<Stream<X>> source,
      @Nonnull final Function<Stream<X>, R> consumer) {
    try (Scope s = scope(opContext);
        Stream<X> stream = source.get()) {
      return consumer.apply(stream);
    }
  }
}
