package com.linkedin.metadata.timeseries;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig.CacheConfig;
import com.linkedin.mxe.GenericAspect;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LatestTimeseriesAspectVersionCachingServiceTest {

  private LatestTimeseriesAspectVersionCachingService cachingService;
  private TimeseriesAspectService mockDelegate;
  private CacheManager cacheManager;
  private ObjectMapper objectMapper;
  private Set<String> cachedAspects;

  @Mock private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    mockDelegate = mock(TimeseriesAspectService.class);
    cacheManager = new ConcurrentMapCacheManager("latestTimeseriesAspect");
    objectMapper = new ObjectMapper();
    cachedAspects = Set.of("datasetProfile");

    CacheConfig cacheConfig = new CacheConfig();
    cacheConfig.setEnabled(true);
    cacheConfig.setTtlHours(48);
    cacheConfig.setTtlJitterMinutes(60);

    cachingService =
        new LatestTimeseriesAspectVersionCachingService(
            mockDelegate, cacheManager, cacheConfig, objectMapper, cachedAspects);

    when(opContext.getObjectMapper()).thenReturn(objectMapper);
  }

  @Test
  public void testGetAspectValuessCacheHit() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,test,PROD)");
    String entityName = urn.getEntityType();
    String aspectName = "datasetProfile";

    EnvelopedAspect cachedAspect = createTestEnvelopedAspect("test_event");
    String cacheKey = String.format("latest:%s:%s:%s", entityName, aspectName, urn);

    // Manually populate cache
    Cache cache = cacheManager.getCache("latestTimeseriesAspect");
    cache.put(cacheKey, RecordUtils.toJsonString(cachedAspect));

    List<EnvelopedAspect> result =
        cachingService.getAspectValues(opContext, urn, entityName, aspectName, null, null, 1, null);

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getAspect().getValue().asString("UTF-8"), "test_event");
    verify(mockDelegate, never())
        .getAspectValues(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testGetAspectValuesCacheMiss() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,test,PROD)");
    String entityName = urn.getEntityType();
    String aspectName = "datasetProfile";

    EnvelopedAspect aspect = createTestEnvelopedAspect("new_event");
    when(mockDelegate.getAspectValues(
            opContext, urn, entityName, aspectName, null, null, 1, null, null))
        .thenReturn(List.of(aspect));

    List<EnvelopedAspect> result =
        cachingService.getAspectValues(opContext, urn, entityName, aspectName, null, null, 1, null);

    assertEquals(result.size(), 1);
    verify(mockDelegate, times(1))
        .getAspectValues(opContext, urn, entityName, aspectName, null, null, 1, null, null);

    // Verify cache was populated
    Cache cache = cacheManager.getCache("latestTimeseriesAspect");
    String cacheKey = String.format("latest:%s:%s:%s", entityName, aspectName, urn);
    assertNotNull(cache.get(cacheKey));
  }

  @Test
  public void testGetLatestTimeseriesAspectValuesCacheHit() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,test,PROD)");
    String aspectName = "datasetProfile";

    EnvelopedAspect cachedAspect = createTestEnvelopedAspect("cached_profile");

    // Populate cache
    Cache cache = cacheManager.getCache("latestTimeseriesAspect");
    String cacheKey = String.format("latest:%s:%s:%s", urn.getEntityType(), aspectName, urn);
    cache.put(cacheKey, RecordUtils.toJsonString(cachedAspect));

    Map<Urn, Map<String, EnvelopedAspect>> result =
        cachingService.getLatestTimeseriesAspectValues(
            opContext, Set.of(urn), Set.of(aspectName), null);

    assertTrue(result.containsKey(urn));
    assertTrue(result.get(urn).containsKey(aspectName));
    verify(mockDelegate, never()).getLatestTimeseriesAspectValues(any(), any(), any(), any());
  }

  @Test
  public void testUpsertDocumentCaches() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,test,PROD)");
    String entityName = urn.getEntityType();
    String aspectName = "datasetProfile";
    String docId = "doc1";

    JsonNode document = createTestDocument(urn.toString(), "profile_data");

    cachingService.upsertDocument(opContext, entityName, aspectName, docId, document);

    verify(mockDelegate).upsertDocument(opContext, entityName, aspectName, docId, document);

    // Verify document was cached
    Cache cache = cacheManager.getCache("latestTimeseriesAspect");
    String cacheKey = String.format("latest:%s:%s:%s", entityName, aspectName, urn);
    assertNotNull(cache.get(cacheKey));
  }

  @Test
  public void testDeleteAspectValuesClearsCache() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,test,PROD)");
    String entityName = urn.getEntityType();
    String aspectName = "datasetProfile";

    EnvelopedAspect aspect = createTestEnvelopedAspect("to_delete");

    // Populate cache
    Cache cache = cacheManager.getCache("latestTimeseriesAspect");
    String cacheKey = String.format("latest:%s:%s:%s", entityName, aspectName, urn);
    cache.put(cacheKey, RecordUtils.toJsonString(aspect));

    assertNotNull(cache.get(cacheKey));

    cachingService.deleteAspectValues(opContext, entityName, aspectName, null);

    assertNull(cache.get(cacheKey));
  }

  private EnvelopedAspect createTestEnvelopedAspect(String eventData) {
    EnvelopedAspect aspect = new EnvelopedAspect();
    GenericAspect genericAspect =
        new GenericAspect()
            .setValue(ByteString.unsafeWrap(eventData.getBytes(StandardCharsets.UTF_8)));
    genericAspect.setContentType("application/json");
    aspect.setAspect(genericAspect);
    return aspect;
  }

  private JsonNode createTestDocument(String urn, String eventData) {
    return objectMapper.createObjectNode().put("urn", urn).put("event", eventData);
  }
}
