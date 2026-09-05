package com.linkedin.gms.factory.config;

import com.linkedin.metadata.config.flags.EnvironmentFeatureProvider;
import com.linkedin.metadata.config.flags.FeatureFlagProvider;
import com.linkedin.metadata.config.flags.FlagEvaluationContextResolver;
import dev.openfeature.sdk.FeatureProvider;
import dev.openfeature.sdk.ImmutableContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Wires {@link FeatureFlagProvider} and the OSS defaults behind it: {@link
 * EnvironmentFeatureProvider} over Spring's {@link Environment}, and a resolver that targets
 * nothing.
 *
 * <p><b>An extension module overrides either default with a {@code @Primary} bean of the same type,
 * and {@code @Primary} is what does the work.</b> The {@link ConditionalOnMissingBean} guards
 * cannot see it: extension beans reach the context through auto-configuration, which Spring Boot
 * processes after all user {@code @Configuration} classes, so at the moment these conditions are
 * evaluated the extension bean is not yet defined. Both beans register and injection succeeds only
 * because the extension one is {@code @Primary} — omit it and startup fails with {@code
 * NoUniqueBeanDefinitionException}. The guards remain for an extension contributed as a user
 * configuration. Same shape as how {@code IndexConventionFactory} wires {@code
 * IndexPrefixResolver}.
 */
@Configuration
public class FeatureFlagProviderFactory {

  /**
   * OSS targets nothing: one deployment, one set of values, so there is no identity to evaluate
   * against.
   */
  @Bean
  @ConditionalOnMissingBean(FlagEvaluationContextResolver.class)
  protected FlagEvaluationContextResolver flagEvaluationContextResolver() {
    return operation -> new ImmutableContext();
  }

  @Bean
  @ConditionalOnMissingBean(FeatureProvider.class)
  protected FeatureProvider featureProvider(final Environment environment) {
    return new EnvironmentFeatureProvider(environment::getProperty);
  }

  @Bean
  protected FeatureFlagProvider featureFlagProvider(
      final FeatureProvider provider, final FlagEvaluationContextResolver contextResolver) {
    return new FeatureFlagProvider(provider, contextResolver);
  }
}
