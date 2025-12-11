/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.candidatesource.DomainsCandidateSource;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({EntitySearchServiceFactory.class})
public class DomainsCandidateSourceFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Bean(name = "domainsCandidateSource")
  @Nonnull
  protected DomainsCandidateSource getInstance(
      final OperationContext opContext, final EntityRegistry entityRegistry) {
    return new DomainsCandidateSource(entityService, entitySearchService, entityRegistry);
  }
}
