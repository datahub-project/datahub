package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.AspectPayloadValidator;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.Database;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class EntityAspectDaoFactory {

  @Autowired(required = false)
  private List<AspectPayloadValidator> payloadValidators;

  @Bean(name = "entityAspectDao")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected AspectDao createEbeanInstance(
      @Qualifier("ebeanServer") final Database server,
      final ConfigurationProvider configurationProvider,
      final MetricUtils metricUtils) {
    EbeanAspectDao ebeanAspectDao =
        new EbeanAspectDao(
            server,
            configurationProvider.getEbean(),
            metricUtils,
            payloadValidators != null ? payloadValidators : List.of(),
            configurationProvider.getDatahub().getValidation() != null
                ? configurationProvider.getDatahub().getValidation().getAspectSize()
                : null);
    if (configurationProvider.getDatahub().isReadOnly()) {
      ebeanAspectDao.setWritable(false);
    }
    return ebeanAspectDao;
  }

  @Bean(name = "entityAspectDao")
  @DependsOn({"cassandraSession"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected AspectDao createCassandraInstance(
      CqlSession session, final ConfigurationProvider configurationProvider) {
    CassandraAspectDao cassandraAspectDao =
        new CassandraAspectDao(
            session,
            payloadValidators != null ? payloadValidators : List.of(),
            configurationProvider.getDatahub().getValidation() != null
                ? configurationProvider.getDatahub().getValidation().getAspectSize()
                : null);
    if (configurationProvider.getDatahub().isReadOnly()) {
      cassandraAspectDao.setWritable(false);
    }
    return cassandraAspectDao;
  }
}
