package com.linkedin.gms.factory.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.semantic.profile.SemanticContentApplyService;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileEligibility;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import io.datahubproject.test.wiring.WiringCoverage;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

/** Loads {@link SemanticContentApplyServiceFactory} so the wiring ratchet records it. */
public class SemanticContentApplyServiceFactoryTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withInitializer(WiringCoverage.initializer())
          .withUserConfiguration(SemanticContentApplyServiceFactory.class)
          .withBean(EntityService.class, () -> mock(EntityService.class))
          .withBean(SemanticProfileRegistry.class, () -> mock(SemanticProfileRegistry.class))
          .withBean(SemanticProfileEligibility.class, () -> mock(SemanticProfileEligibility.class));

  @Test
  public void loadsApplyServiceWhenEntityServiceAndRegistryExist() {
    runner.run(
        context ->
            assertThat(context).hasNotFailed().hasSingleBean(SemanticContentApplyService.class));
  }
}
