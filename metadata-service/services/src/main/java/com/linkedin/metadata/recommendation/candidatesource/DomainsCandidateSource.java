/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DomainsCandidateSource extends EntitySearchAggregationSource {

  private static final String DOMAINS = "domains";

  public DomainsCandidateSource(
      EntityService<?> entityService,
      EntitySearchService entitySearchService,
      EntityRegistry entityRegistry) {
    super(entityService, entitySearchService, entityRegistry);
  }

  @Override
  public String getTitle() {
    return "Domains";
  }

  @Override
  public String getModuleId() {
    return "Domains";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.DOMAIN_SEARCH_LIST;
  }

  @Override
  public boolean isEligible(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME;
  }

  @Override
  protected String getSearchFieldName() {
    return DOMAINS;
  }

  @Override
  protected int getMaxContent() {
    return 10;
  }

  @Override
  protected boolean isValueUrn() {
    return true;
  }
}
