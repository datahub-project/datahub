package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

import com.codahale.metrics.MetricRegistry;
import com.datahub.util.exception.ModelConversionException;
import com.datahub.util.exception.RetryLimitReached;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.entity.TransactionContext;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import io.ebean.DuplicateKeyException;
import io.ebean.ExpressionList;
import io.ebean.Junction;
import io.ebean.PagedList;
import io.ebean.Query;
import io.ebean.RawSql;
import io.ebean.RawSqlBuilder;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import io.ebean.Transaction;
import io.ebean.TxScope;
import io.ebean.annotation.TxIsolation;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Table;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EbeanAspectDao implements AspectDao, AspectMigrationsDao {
  // READ COMMITED is used in conjunction with SELECT FOR UPDATE (read lock) in order
  // to ensure that the aspect's version is not modified outside the transaction.
  // We rely on the retry mechanism if the row is modified and will re-read (require the lock)
  public static final TxIsolation TX_ISOLATION = TxIsolation.READ_COMMITED;

  /** -- GETTER -- Return the server instance used for customized queries. Only used in tests. */
  @Getter private final Database server;

  private boolean connectionValidated = false;

  // Flag used to make sure the dao isn't writing aspects
  // while its storage is being migrated
  private boolean canWrite = true;

  // Why 375? From tuning, this seems to be about the largest size we can get without having ebean
  // batch issues.
  // This may be able to be moved up, 375 is a bit conservative. However, we should be careful to
  // tweak this without
  // more testing.
  private int queryKeysCount = 375; // 0 means no pagination on keys

  private final String batchGetMethod;

  public EbeanAspectDao(@Nonnull final Database server, EbeanConfiguration ebeanConfiguration) {
    this.server = server;
    this.batchGetMethod =
        ebeanConfiguration.getBatchGetMethod() != null
            ? ebeanConfiguration.getBatchGetMethod()
            : "IN";
  }

  @Override
  public void setWritable(boolean canWrite) {
    this.canWrite = canWrite;
  }

  public void setConnectionValidated(boolean validated) {
    connectionValidated = validated;
    canWrite = validated;
  }

  private boolean validateConnection() {
    if (connectionValidated) {
      return true;
    }
    if (!AspectStorageValidationUtil.checkV2TableExists(server)) {
      log.error(
          "GMS is on a newer version than your storage layer. Please refer to "
              + "https://datahubproject.io/docs/advanced/no-code-upgrade to view the upgrade guide.");
      canWrite = false;
      return false;
    } else {
      connectionValidated = true;
      return true;
    }
  }

  @Nonnull
  @Override
  public Optional<EntityAspect> updateAspect(
      @Nullable TransactionContext txContext, @Nonnull SystemAspect aspect) {
    validateConnection();
    if (!canWrite) {
      return Optional.empty();
    }

    EbeanAspectV2 ebeanAspectV2 = EbeanAspectV2.fromEntityAspect(aspect.asLatest());

    saveEbeanAspect(txContext, ebeanAspectV2, false);
    return Optional.of(ebeanAspectV2.toEntityAspect());
  }

  @Override
  @Nonnull
  public Optional<EntityAspect> insertAspect(
      @Nullable TransactionContext txContext, @Nonnull SystemAspect aspect, final long version) {
    validateConnection();
    if (!canWrite) {
      return Optional.empty();
    }

    EbeanAspectV2 ebeanAspectV2 = EbeanAspectV2.fromEntityAspect(aspect.withVersion(version));

    saveEbeanAspect(txContext, ebeanAspectV2, true);
    return Optional.of(ebeanAspectV2.toEntityAspect());
  }

  private void saveEbeanAspect(
      @Nullable TransactionContext txContext,
      @Nonnull final EbeanAspectV2 ebeanAspect,
      final boolean insert) {
    validateConnection();
    if (txContext != null && txContext.tx() != null) {
      if (insert) {
        server.insert(ebeanAspect, txContext.tx());
      } else {
        server.update(ebeanAspect, txContext.tx());
      }
    } else {
      if (insert) {
        server.insert(ebeanAspect);
      } else {
        server.update(ebeanAspect);
      }
    }
  }

  @Nonnull
  @Override
  public Map<String, Map<String, SystemAspect>> getLatestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Map<String, Set<String>> urnAspects,
      boolean forUpdate) {
    validateConnection();

    List<EbeanAspectV2.PrimaryKey> keys =
        urnAspects.entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().stream()
                        .map(
                            aspect ->
                                new EbeanAspectV2.PrimaryKey(
                                    entry.getKey(), aspect, ASPECT_LATEST_VERSION)))
            .collect(Collectors.toList());

    final List<EbeanAspectV2> results;
    if (forUpdate) {
      results = server.find(EbeanAspectV2.class).where().idIn(keys).forUpdate().findList();
    } else {
      results = server.find(EbeanAspectV2.class).where().idIn(keys).findList();
    }

    return toUrnAspectMap(opContext.getEntityRegistry(), results);
  }

  @Override
  public long countEntities() {
    validateConnection();
    return server
        .find(EbeanAspectV2.class)
        .setDistinct(true)
        .select(EbeanAspectV2.URN_COLUMN)
        .findCount();
  }

  @Override
  public boolean checkIfAspectExists(@Nonnull String aspectName) {
    validateConnection();
    return server
        .find(EbeanAspectV2.class)
        .where()
        .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
        .exists();
  }

  @Override
  @Nullable
  public EntityAspect getAspect(
      @Nonnull final String urn, @Nonnull final String aspectName, final long version) {
    return getAspect(new EntityAspectIdentifier(urn, aspectName, version));
  }

  @Override
  @Nullable
  public EntityAspect getAspect(@Nonnull final EntityAspectIdentifier key) {
    validateConnection();
    EbeanAspectV2.PrimaryKey primaryKey =
        new EbeanAspectV2.PrimaryKey(key.getUrn(), key.getAspect(), key.getVersion());
    EbeanAspectV2 ebeanAspect = server.find(EbeanAspectV2.class, primaryKey);
    return ebeanAspect == null ? null : ebeanAspect.toEntityAspect();
  }

  @Override
  public void deleteAspect(
      @Nonnull final Urn urn, @Nonnull final String aspect, @Nonnull final Long version) {
    validateConnection();
    server
        .createQuery(EbeanAspectV2.class)
        .where()
        .eq(EbeanAspectV2.URN_COLUMN, urn.toString())
        .eq(EbeanAspectV2.ASPECT_COLUMN, aspect)
        .eq(EbeanAspectV2.VERSION_COLUMN, version)
        .delete();
  }

  @Override
  public int deleteUrn(@Nullable TransactionContext txContext, @Nonnull final String urn) {
    validateConnection();
    return server
        .createQuery(EbeanAspectV2.class)
        .where()
        .eq(EbeanAspectV2.URN_COLUMN, urn)
        .delete();
  }

  @Override
  @Nonnull
  public Map<EntityAspectIdentifier, EntityAspect> batchGet(
      @Nonnull final Set<EntityAspectIdentifier> keys, boolean forUpdate) {
    validateConnection();
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    final Set<EbeanAspectV2.PrimaryKey> ebeanKeys =
        keys.stream()
            .map(EbeanAspectV2.PrimaryKey::fromAspectIdentifier)
            .collect(Collectors.toSet());
    final List<EbeanAspectV2> records;
    if (queryKeysCount == 0) {
      records = batchGet(ebeanKeys, ebeanKeys.size(), forUpdate);
    } else {
      records = batchGet(ebeanKeys, queryKeysCount, forUpdate);
    }
    return records.stream()
        .collect(
            Collectors.toMap(
                record -> record.getKey().toAspectIdentifier(), EbeanAspectV2::toEntityAspect));
  }

  /**
   * BatchGet that allows pagination on keys to avoid large queries. TODO: can further improve by
   * running the sub queries in parallel
   *
   * @param keys a set of keys with urn, aspect and version
   * @param keysCount the max number of keys for each sub query
   * @param forUpdate whether the operation is intending to write to this row in a tx
   */
  @Nonnull
  private List<EbeanAspectV2> batchGet(
      @Nonnull final Set<EbeanAspectV2.PrimaryKey> keys, final int keysCount, boolean forUpdate) {
    validateConnection();

    int position = 0;

    final int totalPageCount = QueryUtils.getTotalPageCount(keys.size(), keysCount);
    final List<EbeanAspectV2> finalResult =
        batchGetSelectString(new ArrayList<>(keys), keysCount, position, forUpdate);

    while (QueryUtils.hasMore(position, keysCount, totalPageCount)) {
      position += keysCount;
      final List<EbeanAspectV2> oneStatementResult =
          batchGetSelectString(new ArrayList<>(keys), keysCount, position, forUpdate);
      finalResult.addAll(oneStatementResult);
    }

    return finalResult;
  }

  @Nonnull
  private List<EbeanAspectV2> batchGetSelectString(
      @Nonnull final List<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      final int position,
      boolean forUpdate) {

    if (batchGetMethod.equals("IN")) {
      return batchGetIn(keys, keysCount, position, forUpdate);
    }

    return batchGetUnion(keys, keysCount, position, forUpdate);
  }

  /**
   * Builds a single SELECT statement for batch get, which selects one entity, and then can be
   * UNION'd with other SELECT statements.
   */
  private String batchGetSelectString(
      final int selectId,
      @Nonnull final String urn,
      @Nonnull final String aspect,
      final long version,
      @Nonnull final Map<String, Object> outputParamsToValues) {
    validateConnection();

    final String urnArg = "urn" + selectId;
    final String aspectArg = "aspect" + selectId;
    final String versionArg = "version" + selectId;

    outputParamsToValues.put(urnArg, urn);
    outputParamsToValues.put(aspectArg, aspect);
    outputParamsToValues.put(versionArg, version);

    return String.format(
        "SELECT urn, aspect, version, metadata, systemMetadata, createdOn, createdBy, createdFor "
            + "FROM %s WHERE urn = :%s AND aspect = :%s AND version = :%s",
        EbeanAspectV2.class.getAnnotation(Table.class).name(), urnArg, aspectArg, versionArg);
  }

  @Nonnull
  private List<EbeanAspectV2> batchGetUnion(
      @Nonnull final List<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      final int position,
      boolean forUpdate) {
    validateConnection();

    // Build one SELECT per key and then UNION ALL the results. This can be much more performant
    // than OR'ing the
    // conditions together. Our query will look like:
    //   SELECT * FROM metadata_aspect WHERE urn = 'urn0' AND aspect = 'aspect0' AND version = 0
    //   UNION ALL
    //   SELECT * FROM metadata_aspect WHERE urn = 'urn0' AND aspect = 'aspect1' AND version = 0
    //   ...
    // Note: UNION ALL should be safe and more performant than UNION. We're selecting the entire
    // entity key (as well
    // as data), so each result should be unique. No need to deduplicate.
    // Another note: ebean doesn't support UNION ALL, so we need to manually build the SQL statement
    // ourselves.
    final StringBuilder sb = new StringBuilder();
    final int end = Math.min(keys.size(), position + keysCount);
    final Map<String, Object> params = new HashMap<>();
    for (int index = position; index < end; index++) {
      sb.append(
          batchGetSelectString(
              index - position,
              keys.get(index).getUrn(),
              keys.get(index).getAspect(),
              keys.get(index).getVersion(),
              params));

      if (index != end - 1) {
        sb.append(" UNION ALL ");
      }
    }

    // Add FOR UPDATE clause only once at the end of the entire statement
    if (forUpdate) {
      sb.append(" FOR UPDATE");
    }

    final RawSql rawSql =
        RawSqlBuilder.parse(sb.toString())
            .columnMapping(EbeanAspectV2.URN_COLUMN, "key.urn")
            .columnMapping(EbeanAspectV2.ASPECT_COLUMN, "key.aspect")
            .columnMapping(EbeanAspectV2.VERSION_COLUMN, "key.version")
            .create();

    final Query<EbeanAspectV2> query = server.find(EbeanAspectV2.class).setRawSql(rawSql);

    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    return query.findList();
  }

  @Nonnull
  private List<EbeanAspectV2> batchGetIn(
      @Nonnull final List<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      final int position,
      boolean forUpdate) {
    validateConnection();

    // Build a single SELECT with IN clause using composite key comparison
    // Query will look like:
    // SELECT * FROM metadata_aspect WHERE (urn, aspect, version) IN
    // (('urn0', 'aspect0', 0), ('urn1', 'aspect1', 1))
    final StringBuilder sb = new StringBuilder();
    sb.append(
        "SELECT urn, aspect, version, metadata, systemMetadata, createdOn, createdBy, createdFor ");
    sb.append("FROM metadata_aspect_v2 WHERE (urn, aspect, version) IN (");

    final int end = Math.min(keys.size(), position + keysCount);
    final Map<String, Object> params = new HashMap<>();

    for (int index = position; index < end; index++) {
      int paramIndex = index - position;
      String urnParam = "urn" + paramIndex;
      String aspectParam = "aspect" + paramIndex;
      String versionParam = "version" + paramIndex;

      params.put(urnParam, keys.get(index).getUrn());
      params.put(aspectParam, keys.get(index).getAspect());
      params.put(versionParam, keys.get(index).getVersion());

      sb.append("(:" + urnParam + ", :" + aspectParam + ", :" + versionParam + ")");

      if (index != end - 1) {
        sb.append(",");
      }
    }

    sb.append(")");

    if (forUpdate) {
      sb.append(" FOR UPDATE");
    }

    final RawSql rawSql =
        RawSqlBuilder.parse(sb.toString())
            .columnMapping(EbeanAspectV2.URN_COLUMN, "key.urn")
            .columnMapping(EbeanAspectV2.ASPECT_COLUMN, "key.aspect")
            .columnMapping(EbeanAspectV2.VERSION_COLUMN, "key.version")
            .create();

    final Query<EbeanAspectV2> query = server.find(EbeanAspectV2.class).setRawSql(rawSql);

    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    return query.findList();
  }

  @Override
  @Nonnull
  public ListResult<String> listUrns(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize) {

    validateConnection();

    final String urnPrefixMatcher = "urn:li:" + entityName + ":%";
    final PagedList<EbeanAspectV2> pagedList =
        server
            .find(EbeanAspectV2.class)
            .select(EbeanAspectV2.KEY_ID)
            .where()
            .like(EbeanAspectV2.URN_COLUMN, urnPrefixMatcher)
            .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
            .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
            .setFirstRow(start)
            .setMaxRows(pageSize)
            .orderBy()
            .asc(EbeanAspectV2.URN_COLUMN)
            .findPagedList();

    final List<String> urns =
        pagedList.getList().stream()
            .map(entry -> entry.getKey().getUrn())
            .collect(Collectors.toList());

    return toListResult(urns, null, pagedList, start);
  }

  @Nonnull
  @Override
  public Integer countAspect(@Nonnull String aspectName, @Nullable String urnLike) {
    ExpressionList<EbeanAspectV2> exp =
        server
            .find(EbeanAspectV2.class)
            .select(EbeanAspectV2.KEY_ID)
            .where()
            .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
            .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName);

    if (urnLike != null) {
      exp = exp.like(EbeanAspectV2.URN_COLUMN, urnLike);
    }
    return exp.findCount();
  }

  /**
   * Warning this inner Streams must be closed
   *
   * @param args
   * @return
   */
  @Nonnull
  @Override
  public PartitionedStream<EbeanAspectV2> streamAspectBatches(final RestoreIndicesArgs args) {
    ExpressionList<EbeanAspectV2> exp =
        server
            .find(EbeanAspectV2.class)
            .select(EbeanAspectV2.ALL_COLUMNS)
            .where()
            .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION);
    if (args.aspectName != null) {
      exp = exp.eq(EbeanAspectV2.ASPECT_COLUMN, args.aspectName);
    }
    if (args.aspectNames != null && !args.aspectNames.isEmpty()) {
      exp = exp.in(EbeanAspectV2.ASPECT_COLUMN, args.aspectNames);
    }
    if (args.urn != null) {
      exp = exp.eq(EbeanAspectV2.URN_COLUMN, args.urn);
    }
    if (args.urnLike != null) {
      exp = exp.like(EbeanAspectV2.URN_COLUMN, args.urnLike);
    }
    if (args.gePitEpochMs > 0) {
      exp =
          exp.ge(
                  EbeanAspectV2.CREATED_ON_COLUMN,
                  Timestamp.from(Instant.ofEpochMilli(args.gePitEpochMs)))
              .le(
                  EbeanAspectV2.CREATED_ON_COLUMN,
                  Timestamp.from(Instant.ofEpochMilli(args.lePitEpochMs)));
    }

    int start = args.start;
    if (args.urnBasedPagination) {
      start = 0;
      if (args.lastUrn != null && !args.lastUrn.isEmpty()) {
        exp = exp.where().ge(EbeanAspectV2.URN_COLUMN, args.lastUrn);

        // To prevent processing the same aspect multiple times in a restore, it compares against
        // the last aspect if the urn matches the last urn
        if (args.lastAspect != null && !args.lastAspect.isEmpty()) {
          exp =
              exp.where()
                  .and()
                  .or()
                  .ne(EbeanAspectV2.URN_COLUMN, args.lastUrn)
                  .gt(EbeanAspectV2.ASPECT_COLUMN, args.lastAspect);
        }
      }
    }

    if (args.limit > 0) {
      exp = exp.setMaxRows(args.limit);
    }

    return PartitionedStream.<EbeanAspectV2>builder()
        .delegateStream(
            exp.orderBy()
                .asc(EbeanAspectV2.URN_COLUMN)
                .orderBy()
                .asc(EbeanAspectV2.ASPECT_COLUMN)
                .setFirstRow(start)
                .findStream())
        .build();
  }

  /**
   * Warning the stream must be closed
   *
   * @param entityName
   * @param aspectName
   * @return
   */
  @Override
  @Nonnull
  public Stream<EntityAspect> streamAspects(String entityName, String aspectName) {
    ExpressionList<EbeanAspectV2> exp =
        server
            .find(EbeanAspectV2.class)
            .select(EbeanAspectV2.ALL_COLUMNS)
            .where()
            .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
            .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
            .like(EbeanAspectV2.URN_COLUMN, "urn:li:" + entityName + ":%");
    return exp.query().findStream().map(EbeanAspectV2::toEntityAspect);
  }

  @Override
  @Nonnull
  public Iterable<String> listAllUrns(int start, int pageSize) {
    validateConnection();
    PagedList<EbeanAspectV2> ebeanAspects =
        server
            .find(EbeanAspectV2.class)
            .setDistinct(true)
            .select(EbeanAspectV2.URN_COLUMN)
            .orderBy()
            .asc(EbeanAspectV2.URN_COLUMN)
            .setFirstRow(start)
            .setMaxRows(pageSize)
            .findPagedList();
    return ebeanAspects.getList().stream().map(EbeanAspectV2::getUrn).collect(Collectors.toList());
  }

  @Override
  @Nonnull
  public ListResult<String> listAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final long version,
      final int start,
      final int pageSize) {

    validateConnection();

    final String urnPrefixMatcher = "urn:li:" + entityName + ":%";
    final PagedList<EbeanAspectV2> pagedList =
        server
            .find(EbeanAspectV2.class)
            .select(EbeanAspectV2.ALL_COLUMNS)
            .where()
            .like(EbeanAspectV2.URN_COLUMN, urnPrefixMatcher)
            .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
            .eq(EbeanAspectV2.VERSION_COLUMN, version)
            .setFirstRow(start)
            .setMaxRows(pageSize)
            .orderBy()
            .asc(EbeanAspectV2.URN_COLUMN)
            .findPagedList();

    final List<String> aspects =
        pagedList.getList().stream().map(EbeanAspectV2::getMetadata).collect(Collectors.toList());
    final ListResultMetadata listResultMetadata =
        toListResultMetadata(
            pagedList.getList().stream()
                .map(EbeanAspectDao::toExtraInfo)
                .collect(Collectors.toList()));
    return toListResult(aspects, listResultMetadata, pagedList, start);
  }

  @Override
  @Nonnull
  public ListResult<String> listLatestAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize) {

    return listAspectMetadata(entityName, aspectName, ASPECT_LATEST_VERSION, start, pageSize);
  }

  @Override
  @Nonnull
  public <T> Optional<T> runInTransactionWithRetry(
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
      final int maxTransactionRetry) {
    return runInTransactionWithRetry(block, null, maxTransactionRetry);
  }

  @Override
  @Nonnull
  public <T> Optional<T> runInTransactionWithRetry(
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
      @Nullable AspectsBatch batch,
      final int maxTransactionRetry) {

    return runInTransactionWithRetryUnlocked(block, batch, maxTransactionRetry).getResults();
  }

  @Nonnull
  public <T> TransactionResult<T> runInTransactionWithRetryUnlocked(
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
      @Nullable AspectsBatch batch,
      final int maxTransactionRetry) {

    validateConnection();
    TransactionContext transactionContext = TransactionContext.empty(maxTransactionRetry);

    // Default state is rollback
    TransactionResult<T> result = TransactionResult.rollback();
    do {
      try (Transaction transaction =
          server.beginTransaction(TxScope.requiresNew().setIsolation(TX_ISOLATION))) {
        transaction.setBatchMode(true);
        result = block.apply(transactionContext.tx(transaction));
        if (result.isCommitOrRollback()) {
          transaction.commit();
        } else {
          transaction.rollback();
        }
        break;
      } catch (PersistenceException exception) {
        if (exception instanceof DuplicateKeyException) {
          if (batch != null
              && batch.getItems().stream()
                  .allMatch(
                      a ->
                          a.getAspectName()
                              .equals(a.getEntitySpec().getKeyAspectSpec().getName()))) {
            log.warn(
                "Skipping DuplicateKeyException retry since aspect is the key aspect. {}",
                batch.getUrnAspectsMap().keySet());
            break;
          }
        }

        MetricUtils.counter(MetricRegistry.name(this.getClass(), "txFailed")).inc();
        log.warn("Retryable PersistenceException: {}", exception.getMessage());
        transactionContext.addException(exception);
      }
    } while (transactionContext.shouldAttemptRetry());

    if (transactionContext.lastException() != null) {
      MetricUtils.counter(MetricRegistry.name(this.getClass(), "txFailedAfterRetries")).inc();
      throw new RetryLimitReached(
          "Failed to add after " + maxTransactionRetry + " retries",
          transactionContext.lastException());
    }

    return result;
  }

  @Override
  @Nonnull
  public Pair<Long, Long> getVersionRange(
      @Nonnull final String urn, @Nonnull final String aspectName) {
    validateConnection();

    // Use SQL aggregation to get both min and max in a single query
    SqlQuery query =
        server.sqlQuery(
            "SELECT MIN(version) as min_version, MAX(version) as max_version "
                + "FROM metadata_aspect_v2 "
                + "WHERE urn = :urn AND aspect = :aspect");

    query.setParameter("urn", urn);
    query.setParameter("aspect", aspectName);

    SqlRow result = query.findOne();

    if (result == null) {
      return Pair.of(-1L, -1L);
    }

    return Pair.of(result.getLong("min_version"), result.getLong("max_version"));
  }

  @Override
  public long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    return getVersionRange(urn, aspectName).getSecond();
  }

  /**
   * This method is only used as a fallback. It does incur an extra read-lock that is naturally a
   * result of getLatestAspects(, forUpdate=true)
   *
   * @param urnAspects urn and aspect names to fetch
   * @return map of the aspect's next version
   */
  public Map<String, Map<String, Long>> getNextVersions(
      @Nonnull Map<String, Set<String>> urnAspects) {
    validateConnection();

    List<EbeanAspectV2.PrimaryKey> forUpdateKeys = new ArrayList<>();

    // initialize with default next version of 0
    Map<String, Map<String, Long>> result =
        new HashMap<>(
            urnAspects.entrySet().stream()
                .map(
                    entry -> {
                      Map<String, Long> defaultNextVersion = new HashMap<>();
                      entry
                          .getValue()
                          .forEach(
                              aspectName -> {
                                defaultNextVersion.put(aspectName, ASPECT_LATEST_VERSION);
                                forUpdateKeys.add(
                                    new EbeanAspectV2.PrimaryKey(
                                        entry.getKey(), aspectName, ASPECT_LATEST_VERSION));
                              });
                      return Map.entry(entry.getKey(), defaultNextVersion);
                    })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    // forUpdate is required to avoid duplicate key violations (it is used as an indication that the
    // max(version) was invalidated
    server.find(EbeanAspectV2.class).where().idIn(forUpdateKeys).forUpdate().findList();

    Junction<EbeanAspectV2> queryJunction =
        server
            .find(EbeanAspectV2.class)
            .select("urn, aspect, max(version)")
            .where()
            .in("urn", urnAspects.keySet())
            .or();

    ExpressionList<EbeanAspectV2> exp = null;
    for (Map.Entry<String, Set<String>> entry : urnAspects.entrySet()) {
      if (exp == null) {
        exp = queryJunction.and().eq("urn", entry.getKey()).in("aspect", entry.getValue()).endAnd();
      } else {
        exp = exp.and().eq("urn", entry.getKey()).in("aspect", entry.getValue()).endAnd();
      }
    }

    if (exp == null) {
      return result;
    }

    List<EbeanAspectV2.PrimaryKey> dbResults = exp.endOr().findIds();

    for (EbeanAspectV2.PrimaryKey key : dbResults) {
      if (result.get(key.getUrn()).get(key.getAspect()) <= key.getVersion()) {
        result.get(key.getUrn()).put(key.getAspect(), key.getVersion() + 1L);
      }
    }

    return result;
  }

  @Nonnull
  private <T> ListResult<T> toListResult(
      @Nonnull final List<T> values,
      @Nullable final ListResultMetadata listResultMetadata,
      @Nonnull final PagedList<?> pagedList,
      @Nullable final Integer start) {
    final int nextStart =
        (start != null && pagedList.hasNext())
            ? start + pagedList.getList().size()
            : ListResult.INVALID_NEXT_START;
    return ListResult.<T>builder()
        // Format
        .values(values)
        .metadata(listResultMetadata)
        .nextStart(nextStart)
        .hasNext(pagedList.hasNext())
        .totalCount(pagedList.getTotalCount())
        .totalPageCount(pagedList.getTotalPageCount())
        .pageSize(pagedList.getPageSize())
        .build();
  }

  @Nonnull
  private static ExtraInfo toExtraInfo(@Nonnull final EbeanAspectV2 aspect) {
    final ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setVersion(aspect.getKey().getVersion());
    extraInfo.setAudit(toAuditStamp(aspect));
    try {
      extraInfo.setUrn(Urn.createFromString(aspect.getKey().getUrn()));
    } catch (URISyntaxException e) {
      throw new ModelConversionException(e.getMessage());
    }

    return extraInfo;
  }

  @Nonnull
  private static AuditStamp toAuditStamp(@Nonnull final EbeanAspectV2 aspect) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(aspect.getCreatedOn().getTime());

    try {
      auditStamp.setActor(new Urn(aspect.getCreatedBy()));
      if (aspect.getCreatedFor() != null) {
        auditStamp.setImpersonator(new Urn(aspect.getCreatedFor()));
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return auditStamp;
  }

  @Nonnull
  private ListResultMetadata toListResultMetadata(@Nonnull final List<ExtraInfo> extraInfos) {
    final ListResultMetadata listResultMetadata = new ListResultMetadata();
    listResultMetadata.setExtraInfos(new ExtraInfoArray(extraInfos));
    return listResultMetadata;
  }

  @Override
  @Nonnull
  public List<EntityAspect> getAspectsInRange(
      @Nonnull Urn urn, Set<String> aspectNames, long startTimeMillis, long endTimeMillis) {
    validateConnection();
    List<EbeanAspectV2> ebeanAspects =
        server
            .find(EbeanAspectV2.class)
            .select(EbeanAspectV2.ALL_COLUMNS)
            .where()
            .eq(EbeanAspectV2.URN_COLUMN, urn.toString())
            .in(EbeanAspectV2.ASPECT_COLUMN, aspectNames)
            .inRange(
                EbeanAspectV2.CREATED_ON_COLUMN,
                new Timestamp(startTimeMillis),
                new Timestamp(endTimeMillis))
            .findList();
    return ebeanAspects.stream().map(EbeanAspectV2::toEntityAspect).collect(Collectors.toList());
  }

  private static Map<String, SystemAspect> toAspectMap(
      @Nonnull EntityRegistry entityRegistry, Set<EbeanAspectV2> beans) {
    return beans.stream()
        .map(bean -> Map.entry(bean.getAspect(), bean))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> EbeanSystemAspect.builder().forUpdate(e.getValue(), entityRegistry)));
  }

  private static Map<String, Map<String, SystemAspect>> toUrnAspectMap(
      @Nonnull EntityRegistry entityRegistry, Collection<EbeanAspectV2> beans) {
    return beans.stream()
        .collect(Collectors.groupingBy(EbeanAspectV2::getUrn, Collectors.toSet()))
        .entrySet()
        .stream()
        .map(e -> Map.entry(e.getKey(), toAspectMap(entityRegistry, e.getValue())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static String buildMetricName(
      EntitySpec entitySpec, AspectSpec aspectSpec, String status) {
    return String.join(
        MetricUtils.DELIMITER,
        List.of(entitySpec.getName(), aspectSpec.getName(), status.toLowerCase()));
  }

  /**
   * Split batches by the set of Urns, all remaining items go into an `other` batch in the second of
   * the pair
   *
   * @param batch the input batch
   * @param urns urns for batch
   * @return separated batches
   */
  private static Pair<List<AspectsBatch>, AspectsBatch> splitByUrn(
      AspectsBatch batch, Set<Urn> urns, RetrieverContext retrieverContext) {
    Map<Urn, List<MCPItem>> itemsByUrn =
        batch.getMCPItems().stream().collect(Collectors.groupingBy(MCPItem::getUrn));

    AspectsBatch other =
        AspectsBatchImpl.builder()
            .retrieverContext(retrieverContext)
            .items(
                itemsByUrn.entrySet().stream()
                    .filter(entry -> !urns.contains(entry.getKey()))
                    .flatMap(entry -> entry.getValue().stream())
                    .collect(Collectors.toList()))
            .build();

    List<AspectsBatch> nonEmptyBatches =
        urns.stream()
            .map(
                urn ->
                    AspectsBatchImpl.builder()
                        .retrieverContext(retrieverContext)
                        .items(itemsByUrn.get(urn))
                        .build())
            .filter(b -> !b.getItems().isEmpty())
            .collect(Collectors.toList());

    return Pair.of(nonEmptyBatches, other);
  }
}
