package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import io.ebean.PagedList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * An interface specifying create, update, and read operations against metadata entity aspects.
 * This interface is meant to abstract away the storage concerns of these pieces of metadata, permitting any underlying
 * storage system to be used.
 *
 * Requirements for any implementation:
 *    1. Being able to map its internal storage representation to {@link EntityAspect};
 *    2. Honor the internal versioning semantics. The latest version of any aspect is set to 0 for efficient retrieval.
 *       In most cases only the latest state of an aspect will be fetched. See {@link EntityService} for more details.
 *
 * TODO: This interface exposes {@link #runInTransactionWithRetry(Supplier, int)} because {@link EntityService} concerns
 * itself with batching multiple commands into a single transaction. It exposes storage concerns somewhat and it'd be
 * worth looking into ways to move this responsibility inside {@link AspectDao} implementations.
 */
public interface AspectDao {

    @Nullable
    EntityAspect getAspect(@Nonnull final String urn, @Nonnull final String aspectName, final long version);

    @Nullable
    EntityAspect getAspect(@Nonnull final EntityAspectIdentifier key);

    @Nonnull
    Map<EntityAspectIdentifier, EntityAspect> batchGet(@Nonnull final Set<EntityAspectIdentifier> keys);

    @Nonnull
    List<EntityAspect> getAspectsInRange(@Nonnull Urn urn, Set<String> aspectNames, long startTimeMillis, long endTimeMillis);

    @Nullable
    EntityAspect getLatestAspect(@Nonnull final String urn, @Nonnull final String aspectName);

    void saveAspect(
        @Nonnull final String urn,
        @Nonnull final String aspectName,
        @Nonnull final String aspectMetadata,
        @Nonnull final String actor,
        @Nullable final String impersonator,
        @Nonnull final Timestamp timestamp,
        @Nonnull final String systemMetadata,
        final long version,
        final boolean insert);

    void saveAspect(@Nonnull final EntityAspect aspect, final boolean insert);

    long saveLatestAspect(
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

    void deleteAspect(@Nonnull final EntityAspect aspect);

    @Nonnull
    ListResult<String> listUrns(
        @Nonnull final String entityName,
        @Nonnull final String aspectName,
        final int start,
        final int pageSize);

    @Nonnull
    Integer countAspect(
            @Nonnull final String aspectName,
            @Nullable String urnLike);

    @Nonnull
    PagedList<EbeanAspectV2> getPagedAspects(final RestoreIndicesArgs args);

    int deleteUrn(@Nonnull final String urn);

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

    long getNextVersion(@Nonnull final String urn, @Nonnull final String aspectName);

    Map<String, Long> getNextVersions(@Nonnull final String urn, @Nonnull final Set<String> aspectNames);

    long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName);

    void setWritable(boolean canWrite);

    @Nonnull
    <T> T runInTransactionWithRetry(@Nonnull final Supplier<T> block, final int maxTransactionRetry);
}
