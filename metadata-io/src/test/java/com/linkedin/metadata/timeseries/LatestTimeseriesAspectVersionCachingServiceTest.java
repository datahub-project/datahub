package com.linkedin.metadata.timeseries;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig.CacheConfig;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.mxe.GenericAspect;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LatestTimeseriesAspectVersionCachingServiceTest {

  private static final String DATA_CACHE_NAME = "latestTimeseriesAspect";
  private static final String INDEX_CACHE_NAME = "latestTimeseriesAspectIndex";
  private static final String ASPECT_NAME = "datasetProfile";

  private LatestTimeseriesAspectVersionCachingService cachingService;
  private TimeseriesAspectService mockDelegate;
  private CacheManager cacheManager;
  private com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCaffeineData;
  private com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCaffeineIndex;
  private ObjectMapper objectMapper;

  @Mock private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    mockDelegate = mock(TimeseriesAspectService.class);
    objectMapper = new ObjectMapper();

    // Two real Caffeine caches so the production split data/index paths fire correctly.
    // ConcurrentMapCacheManager would skip both branches in putIfNewerNative/evictAspectIndex.
    nativeCaffeineData =
        Caffeine.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).maximumSize(2000).build();
    nativeCaffeineIndex =
        Caffeine.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).maximumSize(2000).build();
    cacheManager =
        caffeineBackedCacheManager(
            DATA_CACHE_NAME, nativeCaffeineData, INDEX_CACHE_NAME, nativeCaffeineIndex);

    CacheConfig cacheConfig = new CacheConfig();
    cacheConfig.setEnabled(true);
    cacheConfig.setTtlHours(48);
    cacheConfig.setTtlJitterMinutes(60);
    cacheConfig.setCachedAspects(Set.of(ASPECT_NAME));

    cachingService =
        new LatestTimeseriesAspectVersionCachingService(
            mockDelegate, cacheManager, cacheConfig, null);

    when(opContext.getObjectMapper()).thenReturn(objectMapper);
  }

  // ---------------------------------------------------------------------------
  // Read path
  // ---------------------------------------------------------------------------

  @Test
  public void testGetAspectValuesCacheHit_wrappedEntry() throws Exception {
    // Cached entries written by the new code path are CachedLatestAspect wrappers.
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,wrapped,PROD)");
    String entityName = urn.getEntityType();

    EnvelopedAspect cachedAspect = createTestEnvelopedAspect(456L, "wrapped_event");
    String cacheKey = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn);
    cacheManager
        .getCache(DATA_CACHE_NAME)
        .put(cacheKey, new CachedLatestAspect(456L, RecordUtils.toJsonString(cachedAspect)));

    List<EnvelopedAspect> result =
        cachingService.getAspectValues(
            opContext, urn, entityName, ASPECT_NAME, null, null, 1, null);

    assertEquals(result.size(), 1);
    verify(mockDelegate, never())
        .getAspectValues(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testGetAspectValuesCacheMiss_populatesCacheWithWrapper() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,miss,PROD)");
    String entityName = urn.getEntityType();

    EnvelopedAspect aspect = createTestEnvelopedAspect(789L, "fetched_event");
    when(mockDelegate.getAspectValues(
            opContext, urn, entityName, ASPECT_NAME, null, null, 1, null, null))
        .thenReturn(List.of(aspect));

    cachingService.getAspectValues(opContext, urn, entityName, ASPECT_NAME, null, null, 1, null);

    String cacheKey = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn);
    Object stored = nativeCaffeineData.getIfPresent(cacheKey);
    assertTrue(
        stored instanceof CachedLatestAspect,
        "cache fill should write a CachedLatestAspect wrapper, got " + stored);
    assertEquals(((CachedLatestAspect) stored).getTimestampMillis(), 789L);
  }

  @Test
  public void testNonLatestQueryBypassesCache() throws Exception {
    // limit > 1 / non-null time bounds / filter / sort should never hit or populate the cache.
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,window,PROD)");
    String entityName = urn.getEntityType();

    when(mockDelegate.getAspectValues(
            eq(opContext),
            eq(urn),
            eq(entityName),
            eq(ASPECT_NAME),
            any(),
            any(),
            any(),
            any(),
            any()))
        .thenReturn(List.of());

    cachingService.getAspectValues(opContext, urn, entityName, ASPECT_NAME, 0L, 1000L, 50, null);

    String cacheKey = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn);
    assertNull(
        nativeCaffeineData.getIfPresent(cacheKey), "windowed queries must not populate the cache");
    verify(mockDelegate, times(1))
        .getAspectValues(opContext, urn, entityName, ASPECT_NAME, 0L, 1000L, 50, null, null);
  }

  // ---------------------------------------------------------------------------
  // Bulk path
  // ---------------------------------------------------------------------------

  @Test
  public void testGetLatestTimeseriesAspectValues_servesCacheableFromCacheFetchesOnlyNonCacheable()
      throws Exception {
    // Caller asks for {cacheable, nonCacheable} for one URN. We must serve the cacheable hit
    // from cache and ONLY ask the delegate for the non-cacheable aspect — the prior code
    // bailed entirely whenever any requested aspect wasn't in cachedAspectNames.
    String nonCacheableAspect = "datasetUsageStatistics";
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,partial,PROD)");

    EnvelopedAspect cachedProfile = createTestEnvelopedAspect(100L, "cached_profile");
    String cacheKey = String.format("latest:%s:%s:%s", urn.getEntityType(), ASPECT_NAME, urn);
    cacheManager
        .getCache(DATA_CACHE_NAME)
        .put(cacheKey, new CachedLatestAspect(100L, RecordUtils.toJsonString(cachedProfile)));

    EnvelopedAspect fetchedUsage = createTestEnvelopedAspect(200L, "fetched_usage");
    when(mockDelegate.getLatestTimeseriesAspectValues(
            eq(opContext), eq(Set.of(urn)), eq(Set.of(nonCacheableAspect)), eq(null)))
        .thenReturn(Map.of(urn, Map.of(nonCacheableAspect, fetchedUsage)));

    Map<Urn, Map<String, EnvelopedAspect>> result =
        cachingService.getLatestTimeseriesAspectValues(
            opContext, Set.of(urn), Set.of(ASPECT_NAME, nonCacheableAspect), null);

    assertTrue(result.get(urn).containsKey(ASPECT_NAME));
    assertTrue(result.get(urn).containsKey(nonCacheableAspect));
    // Critical: the delegate must have been called with ONLY the non-cacheable aspect.
    verify(mockDelegate, times(1))
        .getLatestTimeseriesAspectValues(opContext, Set.of(urn), Set.of(nonCacheableAspect), null);
  }

  @Test
  public void testGetLatestTimeseriesAspectValues_mixedCachedAndUncached() throws Exception {
    // One URN fully cached, another missing — we should serve cached from cache and fetch the
    // missing one from ES, then merge.
    Urn cachedUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,cached,PROD)");
    Urn missingUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,missing,PROD)");

    EnvelopedAspect cachedAspect = createTestEnvelopedAspect(100L, "cached_event");
    EnvelopedAspect fetchedAspect = createTestEnvelopedAspect(200L, "fetched_event");

    String cacheKey =
        String.format("latest:%s:%s:%s", cachedUrn.getEntityType(), ASPECT_NAME, cachedUrn);
    cacheManager
        .getCache(DATA_CACHE_NAME)
        .put(cacheKey, new CachedLatestAspect(100L, RecordUtils.toJsonString(cachedAspect)));

    Map<Urn, Map<String, EnvelopedAspect>> dbResults = new HashMap<>();
    dbResults.put(missingUrn, Map.of(ASPECT_NAME, fetchedAspect));
    when(mockDelegate.getLatestTimeseriesAspectValues(
            eq(opContext), eq(Set.of(missingUrn)), eq(Set.of(ASPECT_NAME)), eq(null)))
        .thenReturn(dbResults);

    Map<Urn, Map<String, EnvelopedAspect>> result =
        cachingService.getLatestTimeseriesAspectValues(
            opContext, Set.of(cachedUrn, missingUrn), Set.of(ASPECT_NAME), null);

    assertEquals(result.size(), 2);
    assertTrue(result.containsKey(cachedUrn));
    assertTrue(result.containsKey(missingUrn));
    // ES was called only for the missing URN.
    verify(mockDelegate, times(1))
        .getLatestTimeseriesAspectValues(opContext, Set.of(missingUrn), Set.of(ASPECT_NAME), null);
  }

  // ---------------------------------------------------------------------------
  // Write path / put-if-newer
  // ---------------------------------------------------------------------------

  @Test
  public void testUpsertDocumentCachesWithTimestamp() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,upsert,PROD)");
    String entityName = urn.getEntityType();

    JsonNode document = createTestDocument(urn.toString(), 1500L, "upsert_event");
    cachingService.upsertDocument(opContext, entityName, ASPECT_NAME, "doc1", document);

    verify(mockDelegate).upsertDocument(opContext, entityName, ASPECT_NAME, "doc1", document);

    String cacheKey = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn);
    Object stored = nativeCaffeineData.getIfPresent(cacheKey);
    assertTrue(stored instanceof CachedLatestAspect);
    assertEquals(((CachedLatestAspect) stored).getTimestampMillis(), 1500L);
  }

  @Test
  public void testUpsertDocument_olderTimestampDoesNotClobberNewer() throws Exception {
    // Out-of-order write: an older event arrives after a newer one is already cached. The
    // newer cached value must survive.
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,ooo,PROD)");
    String entityName = urn.getEntityType();

    JsonNode newer = createTestDocument(urn.toString(), 2000L, "newer_event");
    cachingService.upsertDocument(opContext, entityName, ASPECT_NAME, "doc-newer", newer);

    JsonNode older = createTestDocument(urn.toString(), 1000L, "older_event");
    cachingService.upsertDocument(opContext, entityName, ASPECT_NAME, "doc-older", older);

    String cacheKey = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn);
    CachedLatestAspect stored = (CachedLatestAspect) nativeCaffeineData.getIfPresent(cacheKey);
    assertNotNull(stored);
    assertEquals(
        stored.getTimestampMillis(), 2000L, "newer entry must not be clobbered by older write");
  }

  @Test
  public void testUpsertDocument_missingTimestampEvictsExisting() throws Exception {
    // Establish a cached entry, then upsert a malformed doc with no top-level timestampMillis.
    // The service must evict rather than write a wrapper(ts=0) that an older legitimate event
    // would beat — see the comment on the upsertDocument ts==0 branch.
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,evict,PROD)");
    String entityName = urn.getEntityType();

    cachingService.upsertDocument(
        opContext, entityName, ASPECT_NAME, "d1", createTestDocument(urn.toString(), 2000L, "v1"));
    String cacheKey = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn);
    assertNotNull(nativeCaffeineData.getIfPresent(cacheKey));

    ObjectNode badDoc = objectMapper.createObjectNode().put("urn", urn.toString());
    badDoc.set("event", objectMapper.createObjectNode().put("data", "v2"));
    // intentionally no top-level timestampMillis
    cachingService.upsertDocument(opContext, entityName, ASPECT_NAME, "d2", badDoc);

    assertNull(
        nativeCaffeineData.getIfPresent(cacheKey),
        "missing-timestamp upsert should evict, not clobber with ts=0");
  }

  @Test
  public void testUpsertDocument_missingEventNodeSkipsCacheWrite() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,noevent,PROD)");
    String entityName = urn.getEntityType();

    ObjectNode docNoEvent =
        objectMapper.createObjectNode().put("urn", urn.toString()).put("timestampMillis", 1000L);
    cachingService.upsertDocument(opContext, entityName, ASPECT_NAME, "d", docNoEvent);

    String cacheKey = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn);
    assertNull(
        nativeCaffeineData.getIfPresent(cacheKey),
        "missing event node must not produce a cache entry");
  }

  @Test
  public void testUpsertDocument_newerTimestampOverwrites() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,inorder,PROD)");
    String entityName = urn.getEntityType();

    cachingService.upsertDocument(
        opContext,
        entityName,
        ASPECT_NAME,
        "doc-1",
        createTestDocument(urn.toString(), 1000L, "v1"));
    cachingService.upsertDocument(
        opContext,
        entityName,
        ASPECT_NAME,
        "doc-2",
        createTestDocument(urn.toString(), 2000L, "v2"));

    String cacheKey = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn);
    CachedLatestAspect stored = (CachedLatestAspect) nativeCaffeineData.getIfPresent(cacheKey);
    assertNotNull(stored);
    assertEquals(stored.getTimestampMillis(), 2000L);
  }

  // ---------------------------------------------------------------------------
  // Eviction paths
  // ---------------------------------------------------------------------------

  @Test
  public void testDeleteAspectValuesClearsCacheForAllUrns() throws Exception {
    // Populate via the production write path so the reverse index gets built — then verify
    // delete tears down every URN registered for that (entity, aspect).
    Urn urn1 = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,one,PROD)");
    Urn urn2 = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,two,PROD)");
    String entityName = urn1.getEntityType();

    cachingService.upsertDocument(
        opContext, entityName, ASPECT_NAME, "d1", createTestDocument(urn1.toString(), 100L, "a"));
    cachingService.upsertDocument(
        opContext, entityName, ASPECT_NAME, "d2", createTestDocument(urn2.toString(), 200L, "b"));

    String key1 = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn1);
    String key2 = String.format("latest:%s:%s:%s", entityName, ASPECT_NAME, urn2);
    assertNotNull(nativeCaffeineData.getIfPresent(key1));
    assertNotNull(nativeCaffeineData.getIfPresent(key2));

    cachingService.deleteAspectValues(opContext, entityName, ASPECT_NAME, new Filter());

    assertNull(nativeCaffeineData.getIfPresent(key1), "delete should evict urn1");
    assertNull(nativeCaffeineData.getIfPresent(key2), "delete should evict urn2");
  }

  @Test
  public void testRollbackClearsEntireCache() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,rollback,PROD)");
    cachingService.upsertDocument(
        opContext,
        urn.getEntityType(),
        ASPECT_NAME,
        "d1",
        createTestDocument(urn.toString(), 100L, "x"));

    String cacheKey = String.format("latest:%s:%s:%s", urn.getEntityType(), ASPECT_NAME, urn);
    assertNotNull(nativeCaffeineData.getIfPresent(cacheKey));

    cachingService.rollbackTimeseriesAspects(opContext, "run-1");

    assertNull(nativeCaffeineData.getIfPresent(cacheKey), "rollback should clear the cache");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private EnvelopedAspect createTestEnvelopedAspect(long timestampMillis, String marker)
      throws Exception {
    // Real timeseries events carry timestampMillis at the top level — match that so the
    // service's extractor finds it on cache fill.
    ObjectNode eventNode =
        objectMapper.createObjectNode().put("timestampMillis", timestampMillis).put("data", marker);
    String eventJson = objectMapper.writeValueAsString(eventNode);

    EnvelopedAspect aspect = new EnvelopedAspect();
    GenericAspect generic =
        new GenericAspect()
            .setValue(ByteString.unsafeWrap(eventJson.getBytes(StandardCharsets.UTF_8)));
    generic.setContentType("application/json");
    aspect.setAspect(generic);
    return aspect;
  }

  private JsonNode createTestDocument(String urn, long timestampMillis, String marker) {
    ObjectNode eventNode =
        objectMapper.createObjectNode().put("timestampMillis", timestampMillis).put("data", marker);
    return objectMapper
        .createObjectNode()
        .put("urn", urn)
        .put("timestampMillis", timestampMillis)
        .set("event", eventNode);
  }

  /**
   * Inline {@link CacheManager} that exposes two named Caffeine-backed caches — one for the {@link
   * CachedLatestAspect} data map, one for the reverse index. Needed because the production code
   * branches on {@code instanceof Caffeine}, which {@code ConcurrentMapCacheManager} doesn't
   * satisfy.
   */
  private static CacheManager caffeineBackedCacheManager(
      String dataName,
      com.github.benmanes.caffeine.cache.Cache<Object, Object> dataCaffeine,
      String indexName,
      com.github.benmanes.caffeine.cache.Cache<Object, Object> indexCaffeine) {
    return new CacheManager() {
      @Override
      public Cache getCache(String name) {
        if (dataName.equals(name)) {
          return wrap(name, dataCaffeine);
        }
        if (indexName.equals(name)) {
          return wrap(name, indexCaffeine);
        }
        return null;
      }

      @Override
      public Collection<String> getCacheNames() {
        return List.of(dataName, indexName);
      }

      private Cache wrap(
          String name, com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeine) {
        return new Cache() {
          @Override
          public String getName() {
            return name;
          }

          @Override
          public Object getNativeCache() {
            return caffeine;
          }

          @Override
          public ValueWrapper get(Object key) {
            Object v = caffeine.getIfPresent(key);
            return v == null ? null : () -> v;
          }

          @Override
          public <T> T get(Object key, Class<T> type) {
            Object v = caffeine.getIfPresent(key);
            return v == null ? null : type.cast(v);
          }

          @Override
          public <T> T get(Object key, Callable<T> valueLoader) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void put(Object key, Object value) {
            caffeine.put(key, value);
          }

          @Override
          public void evict(Object key) {
            caffeine.invalidate(key);
          }

          @Override
          public void clear() {
            caffeine.invalidateAll();
          }
        };
      }
    };
  }
}
