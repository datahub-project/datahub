/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.dataproduct;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.DataProductService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DataProductServiceFactory {

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Bean(name = "dataProductService")
  @Scope("singleton")
  @Nonnull
  protected DataProductService getInstance(
      @Qualifier("entityClient") final EntityClient entityClient) throws Exception {
    return new DataProductService(entityClient, _graphClient);
  }
}
