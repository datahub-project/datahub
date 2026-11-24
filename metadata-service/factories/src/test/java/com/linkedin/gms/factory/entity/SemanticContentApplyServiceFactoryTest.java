package com.linkedin.gms.factory.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.semantic.profile.SemanticContentApplyService;
import io.datahubproject.test.wiring.WiringCoverage;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

/** Loads {@link SemanticContentApplyServiceFactory} so the wiring ratchet records it. */
public class SemanticContentApplyServiceFactoryTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withInitializer(WiringCoverage.initializer())
          .withUserConfiguration(
              SemanticContentApplyServiceFactory.class, LateSemanticProfileBeansConfiguration.class)
          .withBean(EntityService.class, () -> mock(EntityService.class));

  @Test
  public void loadsApplyServiceWhenRegistryIsRegisteredAfterTheFactory() {
    runner.run(
        context ->
            assertThat(context).hasNotFailed().hasSingleBean(SemanticContentApplyService.class));
  }
}
