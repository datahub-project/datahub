package com.linkedin.metadata.entity.ebean;

import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import io.ebean.Transaction;
import io.ebean.TxScope;
import javax.annotation.Nonnull;

/**
 * OSS default {@link ScopedTransactionFactory}: pass-through to the single configured {@link
 * Database}, identical to calling it directly. Registered as a bean via {@code
 * EntityAspectDaoFactory}; an extension module may override with a {@code @Primary} bean to add
 * routing.
 */
public class PassThroughScopedTransactionFactory implements ScopedTransactionFactory {

  private static final Scope NOOP_SCOPE = () -> {};

  @Nonnull private final Database server;

  public PassThroughScopedTransactionFactory(@Nonnull Database server) {
    this.server = server;
  }

  @Override
  @Nonnull
  public Transaction begin(@Nonnull OperationContext opContext, @Nonnull TxScope txScope) {
    return server.beginTransaction(txScope);
  }

  @Override
  @Nonnull
  public Scope scope(@Nonnull OperationContext opContext) {
    return NOOP_SCOPE;
  }
}
