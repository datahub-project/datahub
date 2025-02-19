package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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
 * <p>TODO: This interface exposes {@link #runInTransactionWithRetry(Function, int)}
 * (TransactionContext)} because {@link EntityServiceImpl} concerns itself with batching multiple
 * commands into a single transaction. It exposes storage concerns somewhat and it'd be worth
 * looking into ways to move this responsibility inside {@link AspectDao} implementations.
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
      @Nonnull final Set<EntityAspectIdentifier> keys, boolean forUpdate);

  @Nonnull
  List<EntityAspect> getAspectsInRange(
      @Nonnull Urn urn, Set<String> aspectNames, long startTimeMillis, long endTimeMillis);

  /**
   * @param urn urn to fetch
   * @param aspectName aspect to fetch
   * @param forUpdate set to true if the result is used for versioning <a
   *     href="https://ebean.io/docs/query/option#forUpdate">link</a>
   * @return
   */
  @Nullable
  default SystemAspect getLatestAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      boolean forUpdate) {
    return getLatestAspects(opContext, Map.of(urn, Set.of(aspectName)), forUpdate)
        .getOrDefault(urn, Map.of())
        .getOrDefault(aspectName, null);
  }

  /**
   * @param urnAspects urn/aspects to fetch
   * @param forUpdate set to true if the result is used for versioning <a
   *     href="https://ebean.io/docs/query/option#forUpdate">link</a>
   * @return the data
   */
  @Nonnull
  Map<String, Map<String, SystemAspect>> getLatestAspects(
      @Nonnull OperationContext opContext, Map<String, Set<String>> urnAspects, boolean forUpdate);

  /**
   * Updates the system aspect
   *
   * @param txContext transaction context
   * @param aspect the orm model to update
   */
  @Nonnull
  Optional<EntityAspect> updateAspect(
      @Nullable TransactionContext txContext, @Nonnull final SystemAspect aspect);

  /**
   * Insert system aspect, returning the inserted aspect which may be different from the input
   * aspect, having been replaced with an ORM variation.
   *
   * @param txContext transaction context
   * @param aspect the aspect to insert
   */
  @Nonnull
  Optional<EntityAspect> insertAspect(
      @Nullable TransactionContext txContext, @Nonnull final SystemAspect aspect, long version);

  /**
   * Update the latest aspect version 0 and insert the previous version. Returns the updated version
   * 0 aspect which may be different from the input aspect, having been replaced with an ORM
   * variation.
   *
   * @param txContext transaction context
   * @param latestAspect the aspect currently at version 0
   * @param newAspect the new aspect to be inserted or updated at version 0
   */
  default Pair<Optional<EntityAspect>, Optional<EntityAspect>> saveLatestAspect(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nullable SystemAspect latestAspect,
      @Nonnull SystemAspect newAspect) {

    if (newAspect.getSystemMetadataVersion().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Expected a version in systemMetadata.%s", newAspect.getSystemMetadata()));
    }

    if (latestAspect != null && latestAspect.getDatabaseAspect().isPresent()) {

      SystemAspect currentVersion0 = latestAspect.getDatabaseAspect().get();

      // update version 0 with optional write to version N
      long targetVersion =
          nextVersionResolution(currentVersion0.getSystemMetadata(), newAspect.getSystemMetadata());

      // write version N (from previous database state if the version is modified)
      Optional<EntityAspect> inserted = Optional.empty();
      if (!newAspect
          .getSystemMetadataVersion()
          .equals(currentVersion0.getSystemMetadataVersion())) {

        inserted = insertAspect(txContext, latestAspect.getDatabaseAspect().get(), targetVersion);

        // add trace - overwrite if version incremented
        newAspect.setSystemMetadata(opContext.withTraceId(newAspect.getSystemMetadata(), true));
      }

      // update version 0
      Optional<EntityAspect> updated = Optional.empty();
      if (!Objects.equals(currentVersion0.getSystemMetadata(), newAspect.getSystemMetadata())
          || !Objects.equals(currentVersion0.getRecordTemplate(), newAspect.getRecordTemplate())) {
        updated = updateAspect(txContext, newAspect);
      }

      return Pair.of(inserted, updated);
    } else {
      // initial insert
      Optional<EntityAspect> inserted = insertAspect(txContext, newAspect, ASPECT_LATEST_VERSION);
      return Pair.of(Optional.empty(), inserted);
    }
  }

  private long nextVersionResolution(
      @Nullable SystemMetadata currentSystemMetadata, @Nullable SystemMetadata newSystemMetadata) {
    // existing row 0 should have at least version 1
    long currentVersion =
        Optional.ofNullable(currentSystemMetadata)
            .map(SystemMetadata::getVersion)
            .map(Long::valueOf)
            .orElse(1L);
    // new row should have at least version 2 with the expected current version one behind
    long expectedCurrentVersion =
        Optional.ofNullable(newSystemMetadata)
                .map(SystemMetadata::getVersion)
                .map(Long::valueOf)
                .orElse(2L)
            - 1;

    // reconcile the target version based on the max of the two values in case of previous
    // corruption
    long targetVersion = Math.max(1, Math.max(currentVersion, expectedCurrentVersion));

    // if mismatch, fix row 0 metadata
    if (currentVersion != targetVersion) {
      currentSystemMetadata.setVersion(String.valueOf(targetVersion), SetMode.DISALLOW_NULL);
    }

    return targetVersion;
  }

  void deleteAspect(
      @Nonnull final Urn urn, @Nonnull final String aspect, @Nonnull final Long version);

  @Nonnull
  ListResult<String> listUrns(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize);

  @Nonnull
  Integer countAspect(@Nonnull final String aspectName, @Nullable String urnLike);

  @Nonnull
  PartitionedStream<EbeanAspectV2> streamAspectBatches(final RestoreIndicesArgs args);

  @Nonnull
  Stream<EntityAspect> streamAspects(String entityName, String aspectName);

  int deleteUrn(@Nullable TransactionContext txContext, @Nonnull final String urn);

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

  /**
   * Return the min/max version for the given URN & aspect
   *
   * @param urn the urn
   * @param aspectName the aspect
   * @return the range of versions, if they do not exist -1 is returned
   */
  @Nonnull
  Pair<Long, Long> getVersionRange(@Nonnull final String urn, @Nonnull final String aspectName);

  void setWritable(boolean canWrite);

  @Nonnull
  <T> Optional<T> runInTransactionWithRetry(
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
      final int maxTransactionRetry);

  @Nonnull
  default <T> Optional<T> runInTransactionWithRetry(
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
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
