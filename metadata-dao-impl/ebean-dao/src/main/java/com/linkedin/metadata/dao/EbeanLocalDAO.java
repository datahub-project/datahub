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
import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexValue;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.RollbackException;
import lombok.Value;

import static com.linkedin.metadata.dao.EbeanMetadataAspect.*;
import static com.linkedin.metadata.dao.utils.RegisteredUrnPathExtractors.*;


/**
 * An Ebean implementation of {@link BaseLocalDAO}.
 */
public class EbeanLocalDAO<ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseLocalDAO<ASPECT_UNION, URN> {

  private static final String EBEAN_MODEL_PACKAGE = EbeanMetadataAspect.class.getPackage().getName();
  private static final String EBEAN_INDEX_PACKAGE = EbeanMetadataIndex.class.getPackage().getName();

  protected final EbeanServer _server;

  @Value
  static class GMAIndexPair {
    public String valueType;
    public Object value;
  }

  /**
   * Constructor for EbeanLocalDAO
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   */
  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull ServerConfig serverConfig) {
    super(aspectUnionClass, producer);
    _server = createServer(serverConfig);
  }

  /**
   * Constructor for EbeanLocalDAO
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   */
  public EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig) {
    super(producer, storageConfig);
    _server = createServer(serverConfig);
  }

  @Nonnull
  private EbeanServer createServer(@Nonnull ServerConfig serverConfig) {
    // Make sure that the serverConfig includes the package that contains DAO's Ebean model.
    if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
    }
    if (!serverConfig.getPackages().contains(EBEAN_INDEX_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_INDEX_PACKAGE);
    }
    return EbeanServerFactory.create(serverConfig);
  }

  // For testing purpose
  EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull EbeanServer server) {
    super(aspectUnionClass, producer);
    _server = server;
  }

  // For testing purpose
  EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull EbeanServer server, @Nonnull LocalDAOStorageConfig storageConfig) {
    super(producer, storageConfig);
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
  protected <ASPECT extends RecordTemplate> void saveToLocalSecondaryIndex(@Nonnull URN urn,
      @Nonnull ASPECT newValue, long version) {

    // Process and save URN
    // Only do this with the first version of each aspect
    if (version == FIRST_VERSION) {
      processAndSaveUrnToLocalSecondaryIndex(urn);
    }
    processAndSaveAspectToLocalSecondaryIndex(urn, newValue);
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

  protected long saveSingleRecordToLocalSecondaryIndex(@Nonnull URN urn, @Nonnull String aspect,
      @Nonnull String path, @Nonnull Object value) {

    final EbeanMetadataIndex record = new EbeanMetadataIndex()
        .setUrn(urn.toString())
        .setAspect(aspect)
        .setPath(path);
    if (value instanceof Integer || value instanceof Long) {
      record.setLongVal(Long.valueOf(value.toString()));
    } else if (value instanceof Float || value instanceof Double) {
      record.setDoubleVal(Double.valueOf(value.toString()));
    } else {
      record.setStringVal(value.toString());
    }

    _server.insert(record);
    return record.getId();
  }

  @Nonnull
  Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> getStrongConsistentIndexPaths() {
    return Collections.unmodifiableMap(new HashMap<>(_storageConfig.getAspectStorageConfigMap()));
  }

  protected void processAndSaveUrnToLocalSecondaryIndex(@Nonnull URN urn) {
    if (existsInLocalSecondaryIndex(urn)) {
      return;
    }

    final Map<String, Object> pathValueMap = getUrnPathExtractor(urn.getClass()).extractPaths(urn);
    pathValueMap.forEach(
        (path, value) -> saveSingleRecordToLocalSecondaryIndex(urn, urn.getClass().getCanonicalName(), path, value)
    );
  }

  // TODO: Will be implemented later
  protected <ASPECT extends RecordTemplate> void processAndSaveAspectToLocalSecondaryIndex(@Nonnull URN urn,
      @Nullable ASPECT newValue) {

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

  public boolean existsInLocalSecondaryIndex(@Nonnull URN urn) {
    return _server.find(EbeanMetadataIndex.class)
        .where().eq(URN_COLUMN, urn.toString())
        .exists();
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
  private static Urn extractUrn(@Nonnull String urn) {
    try {
      return new Urn(urn);
    } catch (URISyntaxException e) {
      throw new ModelConversionException("Invalid URN: " + urn);
    }
  }

  @Nonnull
  private static Urn extractUrn(@Nonnull EbeanMetadataAspect aspect) {
    return extractUrn(aspect.getKey().getUrn());
  }

  @Nonnull
  private static Urn extractUrn(@Nonnull EbeanMetadataIndex index) {
    return extractUrn(index.getUrn());
  }

  @Nonnull
  private static <ASPECT extends RecordTemplate> ASPECT toRecordTemplate(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull EbeanMetadataAspect aspect) {
    return RecordUtils.toRecordTemplate(aspectClass, aspect.getMetadata());
  }

  @Nonnull
  private <T> ListResult<T> toListResult(@Nonnull List<T> values, @Nullable ListResultMetadata listResultMetadata,
      @Nonnull PagedList<?> pagedList, @Nullable Integer start) {
    final int nextStart = (start != null && pagedList.hasNext()) ? start.intValue() + pagedList.getList().size() : ListResult.INVALID_NEXT_START;
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

  @Nonnull
  static GMAIndexPair getGMAIndexPair(@Nonnull IndexValue indexValue) {
    final Object object;
    if (indexValue.isBoolean()) {
      object = indexValue.getBoolean().toString();
      return new GMAIndexPair(EbeanMetadataIndex.STRING_COLUMN, object);
    }  else if (indexValue.isDouble()) {
      object = indexValue.getDouble();
      return new GMAIndexPair(EbeanMetadataIndex.DOUBLE_COLUMN, object);
    } else if (indexValue.isFloat()) {
      object = indexValue.getFloat();
      return new GMAIndexPair(EbeanMetadataIndex.DOUBLE_COLUMN, object);
    } else if (indexValue.isInt()) {
      object = indexValue.getInt();
      return new GMAIndexPair(EbeanMetadataIndex.LONG_COLUMN, object);
    } else if (indexValue.isLong()) {
      object = indexValue.getLong();
      return new GMAIndexPair(EbeanMetadataIndex.LONG_COLUMN, object);
    } else if (indexValue.isString()) {
      object = indexValue.getString();
      return new GMAIndexPair(EbeanMetadataIndex.STRING_COLUMN, object);
    } else {
      throw new IllegalArgumentException("Invalid index value " + indexValue);
    }
  }

  /**
   * Returns list of urns from strongly consistent secondary index that satisfy the given filter conditions.
   * Results are sorted in increasing alphabetical order of urn.
   * NOTE: Currently this works for only one filter condition
   * TODO: Extend the support for multiple filter conditions
   *
   * @param indexFilter {@link IndexFilter} containing filter conditions to be applied
   * @param lastUrn last urn of the previous fetched page. This eliminates the need to use offset which
   *                 is known to slow down performance of MySQL queries. For the first page, this should be set as NULL
   * @param pageSize maximum number of distinct urns to return
   * @return {@link ListResult} of urns from strongly consistent secondary index that satisfy the given filter conditions
   */
  @Override
  @Nonnull
  public ListResult<Urn> listUrns(@Nonnull IndexFilter indexFilter, @Nullable URN lastUrn, int pageSize) {
    if (!isLocalSecondaryIndexEnabled()) {
      throw new UnsupportedOperationException("Local secondary index isn't supported by EbeanLocalDAO");
    }
    final IndexCriterionArray indexCriterionArray = indexFilter.getCriteria();
    if (indexCriterionArray.size() == 0) {
      throw new UnsupportedOperationException("Empty Index Filter is not supported by EbeanLocalDAO");
    }
    if (indexCriterionArray.size() > 1) {
      throw new UnsupportedOperationException("Currently only one filter condition is supported by EbeanLocalDAO");
    }

    final IndexCriterion criterion = indexCriterionArray.get(0);
    ExpressionList<EbeanMetadataIndex> expressionList = _server.find(EbeanMetadataIndex.class)
        .setDistinct(true)
        .select(EbeanMetadataIndex.URN_COLUMN)
        .where()
        .gt(EbeanMetadataIndex.URN_COLUMN, lastUrn == null ? "" : lastUrn.toString())
        .eq(EbeanMetadataIndex.ASPECT_COLUMN, criterion.getAspect());
    if (criterion.hasPathParams()) {
      final GMAIndexPair gmaIndexPair = getGMAIndexPair(criterion.getPathParams().getValue());
      expressionList = expressionList
          .eq(EbeanMetadataIndex.PATH_COLUMN, criterion.getPathParams().getPath())
          .eq(gmaIndexPair.valueType, gmaIndexPair.value);
    }
    final PagedList<EbeanMetadataIndex> pagedList =  expressionList
        .orderBy()
        .asc(EbeanMetadataIndex.URN_COLUMN)
        .setMaxRows(pageSize)
        .findPagedList();
    final List<Urn> urns = pagedList.getList()
        .stream()
        .map(EbeanLocalDAO::extractUrn)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    return toListResult(urns, null, pagedList, null);
  }
}
