package com.linkedin.gms;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheScheduler;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.TestPropertySource;
import org.testng.annotations.Test;

/** Production {@code application.yaml} with the entity graph cache switched off. */
@TestPropertySource(properties = "datahub.gms.entityGraphCache.enabled=false")
public class SpringEntityGraphCacheDisabledTest extends AbstractGmsSpringContextTest {

  @Autowired
  @Qualifier("entityGraphCache")
  private EntityGraphCache entityGraphCache;

  @Test
  public void testDisabledEntityGraphCacheIsNoOp() {
    assertSame(entityGraphCache, EntityGraphCache.NO_OP);
    assertEquals(applicationContext.getBeanNamesForType(EntityGraphCacheService.class).length, 0);
    assertEquals(applicationContext.getBeanNamesForType(EntityGraphCacheScheduler.class).length, 0);
  }
}
