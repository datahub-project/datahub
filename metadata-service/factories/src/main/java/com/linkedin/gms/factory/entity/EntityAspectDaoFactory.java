/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class EntityAspectDaoFactory {

  @Bean(name = "entityAspectDao")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected AspectDao createEbeanInstance(
      @Qualifier("ebeanServer") final Database server,
      final ConfigurationProvider configurationProvider,
      final MetricUtils metricUtils) {
    EbeanAspectDao ebeanAspectDao =
        new EbeanAspectDao(server, configurationProvider.getEbean(), metricUtils);
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
    CassandraAspectDao cassandraAspectDao = new CassandraAspectDao(session);
    if (configurationProvider.getDatahub().isReadOnly()) {
      cassandraAspectDao.setWritable(false);
    }
    return cassandraAspectDao;
  }
}
