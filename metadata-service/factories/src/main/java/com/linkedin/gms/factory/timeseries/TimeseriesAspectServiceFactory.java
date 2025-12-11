/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.timeseries;

import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({ElasticSearchTimeseriesAspectServiceFactory.class})
public class TimeseriesAspectServiceFactory {
  @Autowired
  @Qualifier("elasticSearchTimeseriesAspectService")
  private ElasticSearchTimeseriesAspectService _elasticSearchTimeseriesAspectService;

  @Bean(name = "timeseriesAspectService")
  @Primary
  @Nonnull
  protected TimeseriesAspectService getInstance() {
    return _elasticSearchTimeseriesAspectService;
  }
}
