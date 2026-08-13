package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.config.cache.RetentionCacheConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.cassandra.CassandraRetentionService;
import com.linkedin.metadata.entity.ebean.AspectTableResolver;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.ScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.retention.RetentionPolicyCache;
import com.linkedin.metadata.entity.retention.SpringRetentionPolicyCache;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class RetentionServiceFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService<ChangeItemImpl> _entityService;

  @Value("${RETENTION_APPLICATION_BATCH_SIZE:1000}")
  private Integer _batchSize;

  @Value("${cache.retention.enabled:true}")
  private boolean retentionCacheEnabled;

  @Value("${cache.retention.ttlSeconds:" + RetentionCacheConfiguration.DEFAULT_TTL_SECONDS + "}")
  private long retentionCacheTtlSeconds;

  @Autowired private ObjectProvider<CacheManager> cacheManagerProvider;

  @Bean(name = "retentionService")
  @DependsOn({"cassandraSession", "entityService"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected RetentionService<ChangeItemImpl> createCassandraInstance(CqlSession session) {
    RetentionService<ChangeItemImpl> retentionService =
        new CassandraRetentionService<>(_entityService, session, _batchSize);
    configurePolicyCache(retentionService);
    _entityService.setRetentionService(retentionService);
    return retentionService;
  }

  @Bean(name = "retentionService")
  @DependsOn("entityService")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected RetentionService<ChangeItemImpl> createEbeanInstance(
      @Qualifier("ebeanServer") final Database server,
      final AspectTableResolver aspectTableResolver,
      final ScopedTransactionFactory scopedTransactionFactory) {
    RetentionService<ChangeItemImpl> retentionService =
        new EbeanRetentionService<>(
            _entityService, server, _batchSize, aspectTableResolver, scopedTransactionFactory);
    configurePolicyCache(retentionService);
    _entityService.setRetentionService(retentionService);
    return retentionService;
  }

  private void configurePolicyCache(RetentionService<ChangeItemImpl> retentionService) {
    if (!retentionCacheEnabled) {
      return;
    }
    CacheManager manager = cacheManagerProvider.getIfAvailable();
    if (manager == null) {
      return;
    }
    Cache cache = manager.getCache(RetentionPolicyCache.CACHE_NAME);
    if (cache == null) {
      return;
    }
    retentionService.setPolicyCache(
        new SpringRetentionPolicyCache(cache, retentionCacheTtlSeconds));
  }
}
