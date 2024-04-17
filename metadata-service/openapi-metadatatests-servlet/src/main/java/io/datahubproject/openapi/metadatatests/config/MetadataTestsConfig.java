package io.datahubproject.openapi.metadatatests.config;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.entities.EntitiesController;
import io.datahubproject.openapi.generated.ScrollTestEntityResponseV2;
import io.datahubproject.openapi.generated.TestEntityRequestV2;
import io.datahubproject.openapi.generated.TestEntityResponseV2;
import io.datahubproject.openapi.metadatatests.delegates.MetadataTestsDelegateImpl;
import io.datahubproject.openapi.metadatatests.generated.controller.MetadataTestApiDelegate;
import io.datahubproject.openapi.v2.delegates.EntityApiDelegateImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetadataTestsConfig {

  @Bean(name = "metadataTestsDelegate")
  public MetadataTestApiDelegate metadataTestsApiDelegate(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      final EntityService<?> entityService,
      final SearchService searchService,
      final EntitySearchService entitySearchService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitiesController entitiesController,
      final QueryEngine queryEngine,
      final ActionApplier actionApplier,
      final PredicateEvaluator predicateEvaluator) {
    final EntityApiDelegateImpl<
            TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2>
        testApiDelegate =
            new EntityApiDelegateImpl<>(
                systemOpContext,
                entityService,
                searchService,
                entitiesController,
                systemOpContext.getAuthorizerContext().getAuthorizer(),
                TestEntityRequestV2.class,
                TestEntityResponseV2.class,
                ScrollTestEntityResponseV2.class);
    return new MetadataTestsDelegateImpl(
        systemOpContext,
        systemOpContext.getAuthorizerContext().getAuthorizer(),
        entityService,
        entitySearchService,
        timeseriesAspectService,
        testApiDelegate,
        queryEngine,
        actionApplier,
        predicateEvaluator);
  }

  @Bean
  public PredicateEvaluator predicateEvaluator() {
    return PredicateEvaluator.getInstance();
  }
}
