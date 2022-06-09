package com.linkedin.gms.factory.test;

import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.test.config.TestDefinitionProvider;
import com.linkedin.metadata.test.eval.UnitTestRuleEvaluator;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({EntityRegistryFactory.class})
public class TestDefinitionProviderFactory {
  @Bean(name = "testDefinitionProvider")
  @Nonnull
  protected TestDefinitionProvider getInstance() {
    return new TestDefinitionProvider(UnitTestRuleEvaluator.getInstance());
  }
}
