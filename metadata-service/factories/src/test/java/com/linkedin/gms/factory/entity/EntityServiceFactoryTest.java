package com.linkedin.gms.factory.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.lock.NoOpEntityWriteLock;
import com.linkedin.metadata.entity.retention.buffer.RetentionBuffer;
import java.lang.reflect.Method;
import java.util.Collections;
import org.springframework.beans.factory.ObjectProvider;
import org.testng.annotations.Test;

public class EntityServiceFactoryTest {

  private EntityService<ChangeItemImpl> build(boolean syncIngestStamping) {
    FeatureFlags featureFlags = new FeatureFlags();
    featureFlags.setPreProcessHooks(new PreProcessHooks());
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    when(provider.getFeatureFlags()).thenReturn(featureFlags);

    @SuppressWarnings("unchecked")
    ObjectProvider<RetentionBuffer> retentionBufferProvider = mock(ObjectProvider.class);
    when(retentionBufferProvider.getIfAvailable()).thenReturn(null);

    return new EntityServiceFactory()
        .createInstance(
            mock(KafkaEventProducer.class),
            mock(AspectDao.class),
            provider,
            false,
            false,
            syncIngestStamping,
            "false",
            "false",
            Collections.emptyList(),
            null,
            retentionBufferProvider,
            new NoOpEntityWriteLock());
  }

  /**
   * entityService.syncIngestStamping must reach the built service. EntityServiceConfiguration is a
   * factory-built parameter object (no Spring binding), so a missing setter call here silently
   * disables the flag in every deployment — exactly the regression this pins against. The getter is
   * intentionally package-private, hence reflection.
   */
  private boolean stampingEnabled(EntityService<ChangeItemImpl> service) throws Exception {
    Method m = EntityServiceImpl.class.getDeclaredMethod("isSyncIngestStampingEnabled");
    m.setAccessible(true);
    return (boolean) m.invoke(service);
  }

  @Test
  public void testSyncIngestStampingWiredThrough() throws Exception {
    assertTrue(stampingEnabled(build(true)));
  }

  @Test
  public void testSyncIngestStampingDefaultsOff() throws Exception {
    assertFalse(stampingEnabled(build(false)));
  }
}
