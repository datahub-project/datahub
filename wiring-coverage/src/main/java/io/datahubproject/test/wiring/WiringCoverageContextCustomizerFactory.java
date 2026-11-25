package io.datahubproject.test.wiring;

import java.util.List;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;

/**
 * Attaches the {@link WiringCoverage} recorder to every context the Spring TestContext framework
 * builds. Registered in {@code META-INF/spring.factories}.
 */
public class WiringCoverageContextCustomizerFactory implements ContextCustomizerFactory {

  @Override
  public ContextCustomizer createContextCustomizer(
      Class<?> testClass, List<ContextConfigurationAttributes> configAttributes) {
    return WiringCoverage.enabled() ? new Customizer() : null;
  }

  /** Stateless, and equal to every other instance so it never splits the test context cache. */
  static final class Customizer implements ContextCustomizer {
    @Override
    public void customizeContext(
        ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {
      WiringCoverage.attach(context);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Customizer;
    }

    @Override
    public int hashCode() {
      return Customizer.class.hashCode();
    }
  }
}
