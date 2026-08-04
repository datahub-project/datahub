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

  /** Closeable scope; {@link #close()} is narrowed to declare no checked exceptions. */
  interface Scope extends AutoCloseable {
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
