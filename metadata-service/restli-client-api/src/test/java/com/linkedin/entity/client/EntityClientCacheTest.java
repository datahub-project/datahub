package com.linkedin.entity.client;

import static org.testng.Assert.assertEquals;

import com.linkedin.entity.EnvelopedAspect;
import org.testng.annotations.Test;

public class EntityClientCacheTest {

  @Test
  public void missUsesShorterTtlThanHit() {
    EnvelopedAspect hit = new EnvelopedAspect();
    EnvelopedAspect miss = new EntityClientCache.NullEnvelopedAspect();

    assertEquals(EntityClientCache.effectiveTtlSeconds(hit, 86400), 86400);
    assertEquals(
        EntityClientCache.effectiveTtlSeconds(miss, 86400),
        EntityClientCache.NEGATIVE_CACHE_TTL_SECONDS);
    assertEquals(EntityClientCache.effectiveTtlSeconds(miss, 0), 0);
    assertEquals(EntityClientCache.effectiveTtlSeconds(miss, 2), 2);
  }
}
