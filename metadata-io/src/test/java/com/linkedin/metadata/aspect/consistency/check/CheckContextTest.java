package com.linkedin.metadata.aspect.consistency;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for CheckContext caching and orphan tracking functionality. */
public class CheckContextTest {

  private CheckContext ctx;
  private OperationContext mockOpContext;
  private EntityService<?> mockEntityService;

  private static final Urn TEST_URN_1 = UrnUtils.getUrn("urn:li:assertion:test-assertion-1");
  private static final Urn TEST_URN_2 = UrnUtils.getUrn("urn:li:assertion:test-assertion-2");
  private static final Urn TEST_URN_3 =
      UrnUtils.getUrn("urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD),mon1)");

  @BeforeMethod
  public void setup() {
    mockOpContext = mock(OperationContext.class);
    mockEntityService = mock(EntityService.class);

    ctx =
        CheckContext.builder()
            .operationContext(mockOpContext)
            .entityService(mockEntityService)
            .graphClient(null)
            .build();
  }

  // ============================================================================
  // Entity Response Cache Tests
  // ============================================================================

  @Test
  public void testCacheEntityResponse() {
    EntityResponse response = new EntityResponse();
    response.setEntityName("assertion");

    ctx.cacheEntityResponse(TEST_URN_1, response);

    EntityResponse cached = ctx.getCachedEntityResponse(TEST_URN_1);
    assertNotNull(cached);
    assertSame(cached, response);
  }

  @Test
  public void testGetCachedEntityResponseReturnsNull() {
    assertNull(ctx.getCachedEntityResponse(TEST_URN_1));
  }

  @Test
  public void testCacheMultipleEntityResponses() {
    EntityResponse response1 = new EntityResponse();
    response1.setEntityName("assertion");
    EntityResponse response2 = new EntityResponse();
    response2.setEntityName("monitor");

    ctx.cacheEntityResponse(TEST_URN_1, response1);
    ctx.cacheEntityResponse(TEST_URN_3, response2);

    assertSame(ctx.getCachedEntityResponse(TEST_URN_1), response1);
    assertSame(ctx.getCachedEntityResponse(TEST_URN_3), response2);
  }

  // ============================================================================
  // Aspect Cache Tests
  // ============================================================================

  @Test
  public void testCacheAspect() {
    Status status = new Status();
    status.setRemoved(false);

    ctx.cacheAspect(TEST_URN_1, "status", status);

    Status cached = ctx.getCachedAspect(TEST_URN_1, "status");
    assertNotNull(cached);
    assertSame(cached, status);
  }

  @Test
  public void testGetCachedAspectReturnsNullForUnknownUrn() {
    RecordTemplate cached = ctx.getCachedAspect(TEST_URN_1, "status");
    assertNull(cached);
  }

  @Test
  public void testGetCachedAspectReturnsNullForUnknownAspect() {
    Status status = new Status();
    ctx.cacheAspect(TEST_URN_1, "status", status);

    RecordTemplate cached = ctx.getCachedAspect(TEST_URN_1, "assertionInfo");
    assertNull(cached);
  }

  @Test
  public void testCacheMultipleAspectsForSameUrn() {
    Status status = new Status();
    status.setRemoved(false);

    // Use a simple RecordTemplate subclass for the second aspect
    Status status2 = new Status();
    status2.setRemoved(true);

    ctx.cacheAspect(TEST_URN_1, "status", status);
    ctx.cacheAspect(TEST_URN_1, "status2", status2);

    assertSame(ctx.getCachedAspect(TEST_URN_1, "status"), status);
    assertSame(ctx.getCachedAspect(TEST_URN_1, "status2"), status2);
  }

  // ============================================================================
  // Orphan URNs Tests
  // ============================================================================

  @Test
  public void testAddOrphanUrns() {
    Set<Urn> orphans = Set.of(TEST_URN_1, TEST_URN_2);

    ctx.addOrphanUrns("assertion", orphans);

    Set<Urn> retrieved = ctx.getOrphanUrns("assertion");
    assertEquals(retrieved.size(), 2);
    assertTrue(retrieved.contains(TEST_URN_1));
    assertTrue(retrieved.contains(TEST_URN_2));
  }

  @Test
  public void testGetOrphanUrnsReturnsEmptyForUnknownType() {
    Set<Urn> orphans = ctx.getOrphanUrns("unknownType");
    assertNotNull(orphans);
    assertTrue(orphans.isEmpty());
  }

  @Test
  public void testHasOrphanUrns() {
    ctx.addOrphanUrns("assertion", Set.of(TEST_URN_1));

    assertTrue(ctx.hasOrphanUrns("assertion"));
    assertFalse(ctx.hasOrphanUrns("monitor"));
  }

  @Test
  public void testAddOrphanUrnsAccumulates() {
    ctx.addOrphanUrns("assertion", Set.of(TEST_URN_1));
    ctx.addOrphanUrns("assertion", Set.of(TEST_URN_2));

    Set<Urn> orphans = ctx.getOrphanUrns("assertion");
    assertEquals(orphans.size(), 2);
    assertTrue(orphans.contains(TEST_URN_1));
    assertTrue(orphans.contains(TEST_URN_2));
  }

  @Test
  public void testOrphanUrnsByMultipleTypes() {
    ctx.addOrphanUrns("assertion", Set.of(TEST_URN_1));
    ctx.addOrphanUrns("monitor", Set.of(TEST_URN_3));

    assertEquals(ctx.getOrphanUrns("assertion").size(), 1);
    assertEquals(ctx.getOrphanUrns("monitor").size(), 1);
    assertTrue(ctx.getOrphanUrns("assertion").contains(TEST_URN_1));
    assertTrue(ctx.getOrphanUrns("monitor").contains(TEST_URN_3));
  }

  // ============================================================================
  // Clear Caches Tests
  // ============================================================================

  @Test
  public void testClearCaches() {
    // Populate all caches
    EntityResponse response = new EntityResponse();
    ctx.cacheEntityResponse(TEST_URN_1, response);

    Status status = new Status();
    ctx.cacheAspect(TEST_URN_1, "status", status);

    ctx.addOrphanUrns("assertion", Set.of(TEST_URN_1));

    // Clear all caches
    ctx.clearCaches();

    // Verify all caches are empty
    assertNull(ctx.getCachedEntityResponse(TEST_URN_1));
    assertNull(ctx.getCachedAspect(TEST_URN_1, "status"));
    assertTrue(ctx.getOrphanUrns("assertion").isEmpty());
    assertFalse(ctx.hasOrphanUrns("assertion"));
  }

  // ============================================================================
  // Check Config Tests
  // ============================================================================

  @Test
  public void testGetCheckConfig() {
    Map<String, Map<String, String>> configs = new HashMap<>();
    configs.put("test-check", Map.of("enabled", "true", "threshold", "10"));

    CheckContext ctxWithConfigs =
        CheckContext.builder()
            .operationContext(mockOpContext)
            .entityService(mockEntityService)
            .checkConfigs(configs)
            .build();

    Map<String, String> config = ctxWithConfigs.getCheckConfig("test-check");
    assertEquals(config.get("enabled"), "true");
    assertEquals(config.get("threshold"), "10");
  }

  @Test
  public void testGetCheckConfigReturnsEmptyForUnknownCheck() {
    Map<String, String> config = ctx.getCheckConfig("unknown-check");
    assertNotNull(config);
    assertTrue(config.isEmpty());
  }

  // ============================================================================
  // Builder Defaults Tests
  // ============================================================================

  @Test
  public void testBuilderDefaults() {
    CheckContext minimalCtx =
        CheckContext.builder()
            .operationContext(mockOpContext)
            .entityService(mockEntityService)
            .build();

    // All cache maps should be initialized but empty
    assertNull(minimalCtx.getCachedEntityResponse(TEST_URN_1));
    assertNull(minimalCtx.getCachedAspect(TEST_URN_1, "status"));
    assertTrue(minimalCtx.getOrphanUrns("assertion").isEmpty());
    assertTrue(minimalCtx.getCheckConfig("any-check").isEmpty());

    // graphClient should be null
    assertNull(minimalCtx.getGraphClient());
  }

  @Test
  public void testBuilderWithAllFields() {
    Map<String, Map<String, String>> configs = Map.of("check1", Map.of("key", "value"));

    CheckContext fullCtx =
        CheckContext.builder()
            .operationContext(mockOpContext)
            .entityService(mockEntityService)
            .graphClient(null)
            .checkConfigs(configs)
            .build();

    assertNotNull(fullCtx.getOperationContext());
    assertNotNull(fullCtx.getEntityService());
    assertEquals(fullCtx.getCheckConfig("check1").get("key"), "value");
  }
}
