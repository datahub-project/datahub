package com.linkedin.gms.factory.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.cassandra.CassandraRetentionService;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.ebean.ScopedTransactionFactory;
import io.ebean.Database;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

/**
 * Loads {@link RetentionServiceFactory} with the production {@code application.yaml} and checks
 * that {@code entityService.impl} picks the retention implementation, and that the chosen one is
 * handed back to the entity service. The databases (Ebean, Cassandra) and the entity service are
 * mocked.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class RetentionServiceFactoryTest {

  private static ApplicationContextRunner runner(EntityService<?> entityService) {
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(RetentionServiceFactory.class)
        .withBean("entityService", EntityService.class, () -> entityService)
        .withBean(
            "systemEntityClient", SystemEntityClient.class, () -> mock(SystemEntityClient.class))
        .withBean("ebeanServer", Database.class, () -> mock(Database.class))
        .withBean(PlainAspectTableResolver.class)
        .withBean(ScopedTransactionFactory.class, () -> mock(ScopedTransactionFactory.class));
  }

  @Test
  public void testProductionDefaultUsesEbeanRetention() {
    EntityService entityService = mock(EntityService.class);
    runner(entityService)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              RetentionService retention =
                  context.getBean("retentionService", RetentionService.class);
              assertThat(retention).isInstanceOf(EbeanRetentionService.class);
              assertThat(((EbeanRetentionService<?>) retention).getEntityService())
                  .isSameAs(entityService);
              verify(entityService).setRetentionService(retention);
            });
  }

  @Test
  public void testCassandraImplUsesCassandraRetention() {
    EntityService entityService = mock(EntityService.class);
    runner(entityService)
        .withPropertyValues("entityService.impl=cassandra")
        .withBean("cassandraSession", CqlSession.class, () -> mock(CqlSession.class))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              RetentionService retention =
                  context.getBean("retentionService", RetentionService.class);
              assertThat(retention).isInstanceOf(CassandraRetentionService.class);
              assertThat(((CassandraRetentionService<?>) retention).getEntityService())
                  .isSameAs(entityService);
              verify(entityService).setRetentionService(retention);
            });
  }

  @Test
  public void testCassandraImplWithoutASessionFailsStartup() {
    runner(mock(EntityService.class))
        .withPropertyValues("entityService.impl=cassandra")
        .run(
            context ->
                assertThat(context)
                    .getFailure()
                    .rootCause()
                    .isInstanceOf(NoSuchBeanDefinitionException.class)
                    .hasMessageContaining("cassandraSession"));
  }
}
