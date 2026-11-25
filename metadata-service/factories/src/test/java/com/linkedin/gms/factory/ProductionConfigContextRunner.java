package com.linkedin.gms.factory;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.StandardEnvironment;

/**
 * An {@link ApplicationContextRunner} whose {@link ConfigurationProvider} is bound from the
 * production {@code application.yaml}, the way GMS binds it, so factory tests exercise the shipped
 * defaults and switch them with {@code withPropertyValues}.
 *
 * <p>The OS environment is dropped as a property source, so variables exported on the host or CI
 * runner (for example {@code AUTH_POLICIES_ENABLED}) cannot change what a test sees as the default.
 */
public final class ProductionConfigContextRunner {

  private ProductionConfigContextRunner() {}

  public static ApplicationContextRunner create() {
    return new ApplicationContextRunner()
        .withInitializer(
            context ->
                context
                    .getEnvironment()
                    .getPropertySources()
                    .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME))
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withUserConfiguration(ConfigurationProvider.class);
  }
}
