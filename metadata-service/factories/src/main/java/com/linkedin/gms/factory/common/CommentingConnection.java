package com.linkedin.gms.factory.common;

import java.sql.Connection;
import javax.annotation.Nonnull;

/**
 * Plain delegating {@link Connection} whose {@code prepareStatement}, {@code prepareCall} and
 * {@code nativeSQL} pass the SQL through {@link ActorSqlComment#prefix(String)}. A concrete class
 * rather than a {@code java.lang.reflect.Proxy} on purpose: the OpenTelemetry Java agent ignores
 * proxy classes and instruments only the outermost {@code Connection}, so a proxy here would hide
 * the agent's own sqlcommenter comment. Plain {@link java.sql.Statement} text is not rewritten;
 * Ebean issues everything through prepared statements. Generated from the {@code
 * java.sql.Connection} abstract methods; default methods are inherited.
 */
final class CommentingConnection implements Connection {
  private final Connection delegate;

  CommentingConnection(@Nonnull Connection delegate) {
    this.delegate = delegate;
  }

  Connection delegate() {
    return delegate;
  }

  @Override
  public java.sql.Statement createStatement() throws java.sql.SQLException {
    return delegate.createStatement();
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(java.lang.String a0)
      throws java.sql.SQLException {
    return delegate.prepareStatement(ActorSqlComment.prefix(a0));
  }

  @Override
  public java.sql.CallableStatement prepareCall(java.lang.String a0) throws java.sql.SQLException {
    return delegate.prepareCall(ActorSqlComment.prefix(a0));
  }

  @Override
  public java.lang.String nativeSQL(java.lang.String a0) throws java.sql.SQLException {
    return delegate.nativeSQL(ActorSqlComment.prefix(a0));
  }

  @Override
  public void setAutoCommit(boolean a0) throws java.sql.SQLException {
    delegate.setAutoCommit(a0);
  }

  @Override
  public boolean getAutoCommit() throws java.sql.SQLException {
    return delegate.getAutoCommit();
  }

  @Override
  public void commit() throws java.sql.SQLException {
    delegate.commit();
  }

  @Override
  public void rollback() throws java.sql.SQLException {
    delegate.rollback();
  }

  @Override
  public void close() throws java.sql.SQLException {
    delegate.close();
  }

  @Override
  public boolean isClosed() throws java.sql.SQLException {
    return delegate.isClosed();
  }

  @Override
  public java.sql.DatabaseMetaData getMetaData() throws java.sql.SQLException {
    return delegate.getMetaData();
  }

  @Override
  public void setReadOnly(boolean a0) throws java.sql.SQLException {
    delegate.setReadOnly(a0);
  }

  @Override
  public boolean isReadOnly() throws java.sql.SQLException {
    return delegate.isReadOnly();
  }

  @Override
  public void setCatalog(java.lang.String a0) throws java.sql.SQLException {
    delegate.setCatalog(a0);
  }

  @Override
  public java.lang.String getCatalog() throws java.sql.SQLException {
    return delegate.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int a0) throws java.sql.SQLException {
    delegate.setTransactionIsolation(a0);
  }

  @Override
  public int getTransactionIsolation() throws java.sql.SQLException {
    return delegate.getTransactionIsolation();
  }

  @Override
  public java.sql.SQLWarning getWarnings() throws java.sql.SQLException {
    return delegate.getWarnings();
  }

  @Override
  public void clearWarnings() throws java.sql.SQLException {
    delegate.clearWarnings();
  }

  @Override
  public java.sql.Statement createStatement(int a0, int a1) throws java.sql.SQLException {
    return delegate.createStatement(a0, a1);
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(java.lang.String a0, int a1, int a2)
      throws java.sql.SQLException {
    return delegate.prepareStatement(ActorSqlComment.prefix(a0), a1, a2);
  }

  @Override
  public java.sql.CallableStatement prepareCall(java.lang.String a0, int a1, int a2)
      throws java.sql.SQLException {
    return delegate.prepareCall(ActorSqlComment.prefix(a0), a1, a2);
  }

  @Override
  public java.util.Map<java.lang.String, java.lang.Class<?>> getTypeMap()
      throws java.sql.SQLException {
    return delegate.getTypeMap();
  }

  @Override
  public void setTypeMap(java.util.Map<java.lang.String, java.lang.Class<?>> a0)
      throws java.sql.SQLException {
    delegate.setTypeMap(a0);
  }

  @Override
  public void setHoldability(int a0) throws java.sql.SQLException {
    delegate.setHoldability(a0);
  }

  @Override
  public int getHoldability() throws java.sql.SQLException {
    return delegate.getHoldability();
  }

  @Override
  public java.sql.Savepoint setSavepoint() throws java.sql.SQLException {
    return delegate.setSavepoint();
  }

  @Override
  public java.sql.Savepoint setSavepoint(java.lang.String a0) throws java.sql.SQLException {
    return delegate.setSavepoint(a0);
  }

  @Override
  public void rollback(java.sql.Savepoint a0) throws java.sql.SQLException {
    delegate.rollback(a0);
  }

  @Override
  public void releaseSavepoint(java.sql.Savepoint a0) throws java.sql.SQLException {
    delegate.releaseSavepoint(a0);
  }

  @Override
  public java.sql.Statement createStatement(int a0, int a1, int a2) throws java.sql.SQLException {
    return delegate.createStatement(a0, a1, a2);
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(java.lang.String a0, int a1, int a2, int a3)
      throws java.sql.SQLException {
    return delegate.prepareStatement(ActorSqlComment.prefix(a0), a1, a2, a3);
  }

  @Override
  public java.sql.CallableStatement prepareCall(java.lang.String a0, int a1, int a2, int a3)
      throws java.sql.SQLException {
    return delegate.prepareCall(ActorSqlComment.prefix(a0), a1, a2, a3);
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(java.lang.String a0, int a1)
      throws java.sql.SQLException {
    return delegate.prepareStatement(ActorSqlComment.prefix(a0), a1);
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(java.lang.String a0, int[] a1)
      throws java.sql.SQLException {
    return delegate.prepareStatement(ActorSqlComment.prefix(a0), a1);
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(java.lang.String a0, java.lang.String[] a1)
      throws java.sql.SQLException {
    return delegate.prepareStatement(ActorSqlComment.prefix(a0), a1);
  }

  @Override
  public java.sql.Clob createClob() throws java.sql.SQLException {
    return delegate.createClob();
  }

  @Override
  public java.sql.Blob createBlob() throws java.sql.SQLException {
    return delegate.createBlob();
  }

  @Override
  public java.sql.NClob createNClob() throws java.sql.SQLException {
    return delegate.createNClob();
  }

  @Override
  public java.sql.SQLXML createSQLXML() throws java.sql.SQLException {
    return delegate.createSQLXML();
  }

  @Override
  public boolean isValid(int a0) throws java.sql.SQLException {
    return delegate.isValid(a0);
  }

  @Override
  public void setClientInfo(java.lang.String a0, java.lang.String a1)
      throws java.sql.SQLClientInfoException {
    delegate.setClientInfo(a0, a1);
  }

  @Override
  public void setClientInfo(java.util.Properties a0) throws java.sql.SQLClientInfoException {
    delegate.setClientInfo(a0);
  }

  @Override
  public java.lang.String getClientInfo(java.lang.String a0) throws java.sql.SQLException {
    return delegate.getClientInfo(a0);
  }

  @Override
  public java.util.Properties getClientInfo() throws java.sql.SQLException {
    return delegate.getClientInfo();
  }

  @Override
  public java.sql.Array createArrayOf(java.lang.String a0, java.lang.Object[] a1)
      throws java.sql.SQLException {
    return delegate.createArrayOf(a0, a1);
  }

  @Override
  public java.sql.Struct createStruct(java.lang.String a0, java.lang.Object[] a1)
      throws java.sql.SQLException {
    return delegate.createStruct(a0, a1);
  }

  @Override
  public void setSchema(java.lang.String a0) throws java.sql.SQLException {
    delegate.setSchema(a0);
  }

  @Override
  public java.lang.String getSchema() throws java.sql.SQLException {
    return delegate.getSchema();
  }

  @Override
  public void abort(java.util.concurrent.Executor a0) throws java.sql.SQLException {
    delegate.abort(a0);
  }

  @Override
  public void setNetworkTimeout(java.util.concurrent.Executor a0, int a1)
      throws java.sql.SQLException {
    delegate.setNetworkTimeout(a0, a1);
  }

  @Override
  public int getNetworkTimeout() throws java.sql.SQLException {
    return delegate.getNetworkTimeout();
  }

  // java.sql.Wrapper: delegated so callers (and the backend-pid lookup) can still reach the driver.
  @Override
  public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {
    return iface.isInstance(this) ? iface.cast(this) : delegate.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws java.sql.SQLException {
    return iface.isInstance(this) || delegate.isWrapperFor(iface);
  }
}
