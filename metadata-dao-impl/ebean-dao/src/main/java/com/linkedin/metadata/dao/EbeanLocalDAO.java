package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import io.ebean.DuplicateKeyException;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.ExpressionList;
import io.ebean.PagedList;
import io.ebean.Transaction;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.RollbackException;

import static com.linkedin.metadata.dao.EbeanMetadataAspect.*;


/**
 * An Ebean implementation of {@link BaseLocalDAO}.
 */
public class EbeanLocalDAO<ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseLocalDAO<ASPECT_UNION, URN> {

  private static final String EBEAN_MODEL_PACKAGE = EbeanMetadataAspect.class.getPackage().getName();

  protected final EbeanServer _server;

  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull ServerConfig serverConfig) {
    super(aspectUnionClass, producer);

    // Make sure that the serverConfig includes the package that contains DAO's Ebean model.
    if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
    }
    _server = EbeanServerFactory.create(serverConfig);
  }

  // For testing purpose
  EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull EbeanServer server) {
    super(aspectUnionClass, producer);
    _server = server;
  }

  /**
   * Return the {@link EbeanServer} server instance used for customized queries.
   */
  public EbeanServer getServer() {
    return _server;
  }

  /**
   * Creates a private in-memory {@link EbeanServer} based on H2 for production.
   */
  @Nonnull
  public static ServerConfig createProductionH2ServerConfig(@Nonnull String dbName) {

    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    String url = "jdbc:h2:mem:" + dbName + ";IGNORECASE=TRUE;DB_CLOSE_DELAY=-1;";
    dataSourceConfig.setUrl(url);
    dataSourceConfig.setDriver("org.h2.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName(dbName);
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);

    return serverConfig;
  }

  /**
   * Creates a private in-memory {@link EbeanServer} based on H2 for testing purpose.
   */
  @Nonnull
  public static ServerConfig createTestingH2ServerConfig() {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    dataSourceConfig.setUrl("jdbc:h2:mem:;IGNORECASE=TRUE;");
    dataSourceConfig.setDriver("org.h2.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(true);
    serverConfig.setDdlRun(true);

    return serverConfig;
  }

  @Override
  protected <T> T runInTransactionWithRetry(@Nonnull Supplier<T> block, int maxTransactionRetry) {
    int retryCount = 0;
    Exception lastException;

    T result = null;
    do {
      try (Transaction transaction = _server.beginTransaction()) {
        result = block.get();
        transaction.commit();
        lastException = null;
        break;
      } catch (RollbackException | DuplicateKeyException exception) {
        lastException = exception;
      }
    } while (++retryCount <= maxTransactionRetry);

    if (lastException != null) {
      throw new RetryLimitReached("Failed to add after " + maxTransactionRetry + " retries", lastException);
    }

    return result;
  }

  @Override
  protected <ASPECT extends RecordTemplate> long saveLatest(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nullable ASPECT oldValue, @Nullable AuditStamp oldAuditStamp, @Nonnull ASPECT newValue, @Nonnull AuditStamp newAuditStamp) {
    // Save oldValue as the largest version + 1
    long largestVersion = 0;
    if (oldValue != null && oldAuditStamp != null) {
      largestVersion = getNextVersion(urn, aspectClass);
      save(urn, oldValue, oldAuditStamp, largestVersion, true);
    }

    // Save newValue as the latest version (v0)
    save(urn, newValue, newAuditStamp, LATEST_VERSION, oldValue == null);
    return largestVersion;
  }

  @Override
  @Nullable
  protected <ASPECT extends RecordTemplate> AspectEntry<ASPECT> getLatest(@Nonnull URN urn,
      @Nonnull Class<ASPECT> aspectClass) {
    final PrimaryKey key = new PrimaryKey(urn.toString(), ModelUtils.getAspectName(aspectClass), 0L);
    final EbeanMetadataAspect latest = _server.find(EbeanMetadataAspect.class, key);
    if (latest == null) {
      return null;
    }

    return new AspectEntry<>(RecordUtils.toRecordTemplate(aspectClass, latest.getMetadata()), toExtraInfo(latest));
  }

  @Override
  protected void save(@Nonnull URN urn, @Nonnull RecordTemplate value, @Nonnull AuditStamp auditStamp, long version,
      boolean insert) {

    final String aspectName = ModelUtils.getAspectName(value.getClass());

    final EbeanMetadataAspect aspect = new EbeanMetadataAspect();
    aspect.setKey(new PrimaryKey(urn.toString(), aspectName, version));
    aspect.setMetadata(RecordUtils.toJsonString(value));
    aspect.setCreatedOn(new Timestamp(auditStamp.getTime()));
    aspect.setCreatedBy(auditStamp.getActor().toString());

    Urn impersonator = auditStamp.getImpersonator();
    if (impersonator != null) {
      aspect.setCreatedFor(impersonator.toString());
    }

    if (insert) {
      _server.insert(aspect);
    } else {
      _server.update(aspect);
    }
  }

  @Override
  protected <ASPECT extends RecordTemplate> long getNextVersion(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {

    final List<PrimaryKey> result = _server.find(EbeanMetadataAspect.class)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .orderBy()
        .desc(VERSION_COLUMN)
        .setMaxRows(1)
        .findIds();

    return result.isEmpty() ? 0 : result.get(0).getVersion() + 1L;
  }

  @Override
  protected <ASPECT extends RecordTemplate> void applyVersionBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull VersionBasedRetention retention, long largestVersion) {
    _server.find(EbeanMetadataAspect.class)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .ne(VERSION_COLUMN, LATEST_VERSION)
        .le(VERSION_COLUMN, largestVersion - retention.getMaxVersionsToRetain() + 1)
        .delete();
  }

  @Override
  protected <ASPECT extends RecordTemplate> void applyTimeBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull TimeBasedRetention retention, long currentTime) {

    _server.find(EbeanMetadataAspect.class)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .lt(CREATED_ON_COLUMN, new Timestamp(currentTime - retention.getMaxAgeToRetain()))
        .delete();
  }

  @Override
  @Nonnull
  public Map<AspectKey<URN, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
      @Nonnull Set<AspectKey<URN, ? extends RecordTemplate>> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    final List<EbeanMetadataAspect> records = batchGet(keys);
    // TODO: Improve this O(n^2) search
    return keys.stream()
        .collect(Collectors.toMap(Function.identity(), key -> records.stream()
            .filter(record -> matchKeys(key, record.getKey()))
            .findFirst()
            .map(record -> toRecordTemplate(key.getAspectClass(), record))));
  }

  @Nonnull
  private List<EbeanMetadataAspect> batchGet(@Nonnull Set<AspectKey<URN, ? extends RecordTemplate>> keys) {

    ExpressionList<EbeanMetadataAspect> query = _server.find(EbeanMetadataAspect.class).select(ALL_COLUMNS).where();
    if (keys.size() > 1) {
      query = query.or();
    }

    for (AspectKey<URN, ? extends RecordTemplate> key : keys) {
      query = query.and()
          .eq(URN_COLUMN, key.getUrn().toString())
          .eq(ASPECT_COLUMN, ModelUtils.getAspectName(key.getAspectClass()))
          .eq(VERSION_COLUMN, key.getVersion())
          .endAnd();
    }

    return query.findList();
  }

  /**
   * Checks if an {@link AspectKey} and a {@link PrimaryKey} for Ebean are equivalent
   * @param aspectKey Urn needs to do a ignore case match
   */
  private boolean matchKeys(@Nonnull AspectKey<URN, ? extends RecordTemplate> aspectKey, @Nonnull PrimaryKey pk) {
    return aspectKey.getUrn().toString().equalsIgnoreCase(pk.getUrn()) && aspectKey.getVersion() == pk.getVersion()
        && ModelUtils.getAspectName(aspectKey.getAspectClass()).equals(pk.getAspect());
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<Long> listVersions(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, int start, int pageSize) {

    checkValidAspect(aspectClass);

    final PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
        .select(KEY_ID)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(VERSION_COLUMN)
        .findPagedList();

    final List<Long> versions =
        pagedList.getList().stream().map(a -> a.getKey().getVersion()).collect(Collectors.toList());
    return toListResult(versions, null, pagedList, start);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<Urn> listUrns(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize) {

    checkValidAspect(aspectClass);

    final PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
        .select(KEY_ID)
        .where()
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .eq(VERSION_COLUMN, LATEST_VERSION)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(URN_COLUMN)
        .findPagedList();

    final List<Urn> urns = pagedList.getList().stream().map(EbeanLocalDAO::extractUrn).collect(Collectors.toList());
    return toListResult(urns, null, pagedList, start);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, @Nonnull URN urn,
      int start, int pageSize) {

    checkValidAspect(aspectClass);

    final PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
        .select(ALL_COLUMNS)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(VERSION_COLUMN)
        .findPagedList();

    final List<ASPECT> aspects =
        pagedList.getList().stream().map(a -> toRecordTemplate(aspectClass, a)).collect(Collectors.toList());
    final ListResultMetadata listResultMetadata =
        makeListResultMetadata(pagedList.getList().stream().map(this::toExtraInfo).collect(Collectors.toList()));
    return toListResult(aspects, listResultMetadata, pagedList, start);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, long version,
      int start, int pageSize) {

    checkValidAspect(aspectClass);

    final PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
        .select(ALL_COLUMNS)
        .where()
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .eq(VERSION_COLUMN, version)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(URN_COLUMN)
        .findPagedList();

    final List<ASPECT> aspects =
        pagedList.getList().stream().map(a -> toRecordTemplate(aspectClass, a)).collect(Collectors.toList());
    final ListResultMetadata listResultMetadata =
        makeListResultMetadata(pagedList.getList().stream().map(this::toExtraInfo).collect(Collectors.toList()));
    return toListResult(aspects, listResultMetadata, pagedList, start);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize) {
    return list(aspectClass, LATEST_VERSION, start, pageSize);
  }

  @Nonnull
  private static Urn extractUrn(@Nonnull EbeanMetadataAspect aspect) {
    final String urn = aspect.getKey().getUrn();
    try {
      return new Urn(urn);
    } catch (URISyntaxException e) {
      throw new ModelConversionException("Invalid URN: " + urn);
    }
  }

  @Nonnull
  private static <ASPECT extends RecordTemplate> ASPECT toRecordTemplate(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull EbeanMetadataAspect aspect) {
    return RecordUtils.toRecordTemplate(aspectClass, aspect.getMetadata());
  }

  @Nonnull
  private <T> ListResult<T> toListResult(@Nonnull List<T> values, @Nullable ListResultMetadata listResultMetadata,
      @Nonnull PagedList<?> pagedList, int start) {
    final int nextStart = pagedList.hasNext() ? start + pagedList.getList().size() : ListResult.INVALID_NEXT_START;
    return ListResult.<T>builder()
        // Format
        .values(values)
        .metadata(listResultMetadata)
        .nextStart(nextStart)
        .havingMore(pagedList.hasNext())
        .totalCount(pagedList.getTotalCount())
        .totalPageCount(pagedList.getTotalPageCount())
        .pageSize(pagedList.getPageSize())
        .build();
  }

  @Nonnull
  private ExtraInfo toExtraInfo(@Nonnull EbeanMetadataAspect aspect) {
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

    final ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setVersion(aspect.getKey().getVersion());
    extraInfo.setAudit(auditStamp);
    try {
      extraInfo.setUrn(Urn.createFromString(aspect.getKey().getUrn()));
    } catch (URISyntaxException e) {
      throw new ModelConversionException(e.getMessage());
    }

    return extraInfo;
  }

  @Nonnull
  private ListResultMetadata makeListResultMetadata(@Nonnull List<ExtraInfo> extraInfos) {
    final ListResultMetadata listResultMetadata = new ListResultMetadata();
    listResultMetadata.setExtraInfos(new ExtraInfoArray(extraInfos));
    return listResultMetadata;
  }

  @Override
  public long newNumericId(@Nonnull String namespace, int maxTransactionRetry) {
    return runInTransactionWithRetry(() -> {
      final Optional<EbeanMetadataId> result = _server.find(EbeanMetadataId.class)
          .where()
          .eq(EbeanMetadataId.NAMESPACE_COLUMN, namespace)
          .orderBy()
          .desc(EbeanMetadataId.ID_COLUMN)
          .setMaxRows(1)
          .findOneOrEmpty();

      EbeanMetadataId id = result.orElse(new EbeanMetadataId(namespace, 0));
      id.setId(id.getId() + 1);
      _server.insert(id);
      return id;
    }, maxTransactionRetry).getId();
  }
}
