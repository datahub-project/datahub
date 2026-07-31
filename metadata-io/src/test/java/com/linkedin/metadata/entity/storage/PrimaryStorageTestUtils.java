package com.linkedin.metadata.entity.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.metadata.context.StorageTarget;
import io.ebean.Database;
import javax.annotation.Nonnull;

/** Builds {@link PrimaryStorageResolver} instances for unit and integration tests. */
public final class PrimaryStorageTestUtils {

  private PrimaryStorageTestUtils() {}

  @Nonnull
  public static PrimaryStorageResolver ebeanResolver(@Nonnull Database primaryDatabase) {
    return PrimaryStorageResolver.forSingleEbeanDatabase(primaryDatabase);
  }

  @Nonnull
  public static PrimaryStorageResolver cassandraResolver(@Nonnull CqlSession primarySession) {
    return PrimaryStorageResolver.forSingleCassandraSession(primarySession);
  }

  /** Split-pool resolver: same database/cluster, separate PRIMARY and READ pools. */
  @Nonnull
  public static PrimaryStorageResolver splitPoolEbeanResolver(
      @Nonnull Database primaryDatabase, @Nonnull Database readPoolDatabase) {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, primaryDatabase);
    registry.register(StorageTarget.READ, readPoolDatabase);
    return new PrimaryStorageResolver(registry, ReadPreference.READ);
  }

  @Nonnull
  public static PrimaryStorageResolver splitPoolCassandraResolver(
      @Nonnull CqlSession primarySession, @Nonnull CqlSession readPoolSession) {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, primarySession);
    registry.register(StorageTarget.READ, readPoolSession);
    return new PrimaryStorageResolver(registry, ReadPreference.READ);
  }
}
