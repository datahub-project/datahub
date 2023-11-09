package io.datahubproject.openapi.metadatatests.config;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import io.datahubproject.openapi.delegates.EntityApiDelegateImpl;
import io.datahubproject.openapi.entities.EntitiesController;
import io.datahubproject.openapi.generated.ScrollTestEntityResponseV2;
import io.datahubproject.openapi.generated.TestEntityRequestV2;
import io.datahubproject.openapi.generated.TestEntityResponseV2;
import io.datahubproject.openapi.metadatatests.generated.controller.MetadataTestApiDelegate;
import io.datahubproject.openapi.metadatatests.delegates.MetadataTestsDelegateImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.datahub.authorization.AuthorizerChain;

@Configuration
public class MetadataTestsConfig {

    @Bean(name = "metadataTestsDelegate")
    public MetadataTestApiDelegate metadataTestsApiDelegate(final EntityService entityService, final SearchService searchService,
                                                            final EntitySearchService entitySearchService, final EntitiesController entitiesController,
                                                            @Value("${authorization.restApiAuthorization:false}") final boolean restApiAuthorizationEnabled,
                                                            final AuthorizerChain authorizationChain, final QueryEngine queryEngine,
                                                            final ActionApplier actionApplier, final PredicateEvaluator predicateEvaluator) {
        final EntityApiDelegateImpl<TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2> testApiDelegate
                = new EntityApiDelegateImpl<>(entityService, searchService, entitiesController, restApiAuthorizationEnabled,
                authorizationChain, TestEntityRequestV2.class, TestEntityResponseV2.class, ScrollTestEntityResponseV2.class);
        return new MetadataTestsDelegateImpl(entityService, entitySearchService, testApiDelegate, restApiAuthorizationEnabled,
                authorizationChain, queryEngine, actionApplier, predicateEvaluator);
    }

    @Bean
    public PredicateEvaluator predicateEvaluator() {
        return PredicateEvaluator.getInstance();
    }
}
