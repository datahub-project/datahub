package com.linkedin.entity.client;

import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_KEY_ASPECT_NAME;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class EntityClientCacheTest {

  private static final Urn URN_A = UrnUtils.getUrn("urn:li:corpuser:entityClientCacheTestA");
  private static final Urn URN_B = UrnUtils.getUrn("urn:li:corpuser:entityClientCacheTestB");
  private static final Set<String> ASPECTS =
      Set.of(CORP_USER_INFO_ASPECT_NAME, CORP_USER_KEY_ASPECT_NAME);

  private static EntityResponse emptyResponse(Urn urn) {
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(urn.getEntityType())
        .setAspects(new EnvelopedAspectMap());
  }

  private static EntityClientCache buildCache(boolean enabled) {
    EntityClientCacheConfig config = new EntityClientCacheConfig();
    config.setEnabled(enabled);
    config.setMaxBytes(100_000);
    config.setEntityAspectTTLSeconds(
        Map.of("corpuser", Map.of(CORP_USER_INFO_ASPECT_NAME, 60, CORP_USER_KEY_ASPECT_NAME, 60)));

    Function<EntityClientCache.CollectionKey, Map<Urn, EntityResponse>> fetch =
        collectionKey ->
            collectionKey.getUrns().stream()
                .collect(Collectors.toMap(urn -> urn, EntityClientCacheTest::emptyResponse));

    return EntityClientCache.builder()
        .config(config)
        .build(fetch, null, EntityClientCacheTest.class);
  }

  private static EntityClientCache.Key key(OperationContext opContext, Urn urn, String aspectName) {
    return EntityClientCache.Key.builder()
        .contextId(opContext.getEntityContextId())
        .urn(urn)
        .aspectName(aspectName)
        .build();
  }

  @Test
  public void testBatchInvalidateEvictsOnlyRequestedUrnAspectPairs() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    EntityClientCache cache = buildCache(true);

    // Populate: two urns x two aspects.
    cache.batchGetV2(opContext, Set.of(URN_A, URN_B), ASPECTS);

    // Invalidate only corpUserInfo for URN_A.
    cache.invalidate(Map.of(URN_A, Set.of(CORP_USER_INFO_ASPECT_NAME)));

    Set<EntityClientCache.Key> remaining = cache.getCache().keySet();
    assertFalse(
        remaining.contains(key(opContext, URN_A, CORP_USER_INFO_ASPECT_NAME)),
        "invalidated (URN_A, corpUserInfo) should be evicted");
    assertTrue(
        remaining.contains(key(opContext, URN_A, CORP_USER_KEY_ASPECT_NAME)),
        "unrelated aspect on same urn should remain");
    assertTrue(
        remaining.contains(key(opContext, URN_B, CORP_USER_INFO_ASPECT_NAME)),
        "same aspect on a different urn should remain");
  }

  @Test
  public void testBatchInvalidateIsNoOpWhenCacheDisabled() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    EntityClientCache cache = buildCache(false);

    // With caching disabled, reads bypass the cache, so there is nothing to evict and the call
    // must simply not throw.
    cache.batchGetV2(opContext, Set.of(URN_A), ASPECTS);
    cache.invalidate(Map.of(URN_A, Set.of(CORP_USER_INFO_ASPECT_NAME)));

    assertTrue(cache.getCache().keySet().isEmpty(), "disabled cache holds no entries");
  }
}
