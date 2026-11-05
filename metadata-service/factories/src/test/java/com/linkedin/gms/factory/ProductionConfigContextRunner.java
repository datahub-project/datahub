package com.linkedin.gms.factory;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * An {@link ApplicationContextRunner} whose {@link ConfigurationProvider} is bound from the
 * production {@code application.yaml}, the way GMS binds it, so factory tests exercise the shipped
 * defaults and switch them with {@code withPropertyValues}.
 */
public final class ProductionConfigContextRunner {

  private ProductionConfigContextRunner() {}

  public static ApplicationContextRunner create() {
    return new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withUserConfiguration(ConfigurationProvider.class);
  }
}
