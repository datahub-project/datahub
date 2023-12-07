package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.transactions.AspectsBatch;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.PagedList;
import io.ebean.Transaction;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An interface specifying create, update, and read operations against metadata entity aspects. This
 * interface is meant to abstract away the storage concerns of these pieces of metadata, permitting
 * any underlying storage system to be used.
 *
 * <p>Requirements for any implementation: 1. Being able to map its internal storage representation
 * to {@link EntityAspect}; 2. Honor the internal versioning semantics. The latest version of any
 * aspect is set to 0 for efficient retrieval. In most cases only the latest state of an aspect will
 * be fetched. See {@link EntityServiceImpl} for more details.
 *
 * <p>TODO: This interface exposes {@link #runInTransactionWithRetry(Supplier, int)} because {@link
 * EntityServiceImpl} concerns itself with batching multiple commands into a single transaction. It
 * exposes storage concerns somewhat and it'd be worth looking into ways to move this responsibility
 * inside {@link AspectDao} implementations.
 */
public interface AspectDao {
  String ASPECT_WRITE_COUNT_METRIC_NAME = "aspectWriteCount";
  String ASPECT_WRITE_BYTES_METRIC_NAME = "aspectWriteBytes";

  @Nullable
  EntityAspect getAspect(
      @Nonnull final String urn, @Nonnull final String aspectName, final long version);

  @Nullable
  EntityAspect getAspect(@Nonnull final EntityAspectIdentifier key);

  @Nonnull
  Map<EntityAspectIdentifier, EntityAspect> batchGet(
      @Nonnull final Set<EntityAspectIdentifier> keys);

  @Nonnull
  List<EntityAspect> getAspectsInRange(
      @Nonnull Urn urn, Set<String> aspectNames, long startTimeMillis, long endTimeMillis);

  @Nullable
  default EntityAspect getLatestAspect(
      @Nonnull final String urn, @Nonnull final String aspectName) {
    return getLatestAspects(Map.of(urn, Set.of(aspectName)))
        .getOrDefault(urn, Map.of())
        .getOrDefault(aspectName, null);
  }

  @Nonnull
  Map<String, Map<String, EntityAspect>> getLatestAspects(Map<String, Set<String>> urnAspects);

  void saveAspect(
      @Nullable Transaction tx,
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final String aspectMetadata,
      @Nonnull final String actor,
      @Nullable final String impersonator,
      @Nonnull final Timestamp timestamp,
      @Nonnull final String systemMetadata,
      final long version,
      final boolean insert);

  void saveAspect(
      @Nullable Transaction tx, @Nonnull final EntityAspect aspect, final boolean insert);

  long saveLatestAspect(
      @Nullable Transaction tx,
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nullable final String oldAspectMetadata,
      @Nullable final String oldActor,
      @Nullable final String oldImpersonator,
      @Nullable final Timestamp oldTime,
      @Nullable final String oldSystemMetadata,
      @Nonnull final String newAspectMetadata,
      @Nonnull final String newActor,
      @Nullable final String newImpersonator,
      @Nonnull final Timestamp newTime,
      @Nullable final String newSystemMetadata,
      final Long nextVersion);

  void deleteAspect(@Nullable Transaction tx, @Nonnull final EntityAspect aspect);

  @Nonnull
  ListResult<String> listUrns(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize);

  @Nonnull
  Integer countAspect(@Nonnull final String aspectName, @Nullable String urnLike);

  @Nonnull
  PagedList<EbeanAspectV2> getPagedAspects(final RestoreIndicesArgs args);

  @Nonnull
  Stream<EntityAspect> streamAspects(String entityName, String aspectName);

  int deleteUrn(@Nullable Transaction tx, @Nonnull final String urn);

  @Nonnull
  ListResult<String> listLatestAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize);

  @Nonnull
  ListResult<String> listAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final long version,
      final int start,
      final int pageSize);

  Map<String, Map<String, Long>> getNextVersions(@Nonnull Map<String, Set<String>> urnAspectMap);

  default long getNextVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    return getNextVersions(urn, Set.of(aspectName)).get(aspectName);
  }

  default Map<String, Long> getNextVersions(
      @Nonnull final String urn, @Nonnull final Set<String> aspectNames) {
    return getNextVersions(Map.of(urn, aspectNames)).get(urn);
  }

  long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName);

  void setWritable(boolean canWrite);

  @Nonnull
  <T> T runInTransactionWithRetry(
      @Nonnull final Function<Transaction, T> block, final int maxTransactionRetry);

  @Nonnull
  default <T> T runInTransactionWithRetry(
      @Nonnull final Function<Transaction, T> block,
      AspectsBatch batch,
      final int maxTransactionRetry) {
    return runInTransactionWithRetry(block, maxTransactionRetry);
  }

  default void incrementWriteMetrics(String aspectName, long count, long bytes) {
    MetricUtils.counter(
            this.getClass(),
            String.join(MetricUtils.DELIMITER, List.of(ASPECT_WRITE_COUNT_METRIC_NAME, aspectName)))
        .inc(count);
    MetricUtils.counter(
            this.getClass(),
            String.join(MetricUtils.DELIMITER, List.of(ASPECT_WRITE_BYTES_METRIC_NAME, aspectName)))
        .inc(bytes);
  }
}
