package com.linkedin.gms.factory.plugins;

import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import com.linkedin.metadata.aspect.hooks.testfixtures.EmbedV1ToV2Mutator;
import com.linkedin.metadata.aspect.hooks.testfixtures.EmbedV2ToV3Mutator;
import com.linkedin.metadata.aspect.hooks.testfixtures.EmbedV3ToV4Mutator;
import com.linkedin.metadata.aspect.hooks.testfixtures.GlobalTagsV1ToV2Mutator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Test-only Spring configuration — wires the {@link AspectMigrationMutator} test fixtures into the
 * Spring context so the ZDU smoke-test framework can exercise the {@link
 * com.linkedin.metadata.aspect.hooks.AspectMigrationMutatorChain} end-to-end.
 *
 * <p>Loaded only when the Spring property {@code zdu.test.framework.enabled=true}. The property is
 * bound from the {@code ZDU_TEST_FRAMEWORK_ENABLED} environment variable via Spring's relaxed
 * binding — no entry in {@code application.yaml} is required, so production config has no
 * declaration of this flag at all. The ZDU smoke-test framework sets the env var on GMS and
 * upgrade-job containers; production deployments leave it unset (default missing ⇒ condition {@code
 * false} ⇒ class not instantiated).
 *
 * <p>Production safety: this flag is intentionally namespaced separately from the existing
 * production-facing {@code featureFlags.aspectMigrationMutatorEnabled} flag. Even if an operator
 * enables the production flag (e.g., to roll out a real future aspect migration), this test fixture
 * config remains dormant unless they also explicitly opt in via {@code
 * ZDU_TEST_FRAMEWORK_ENABLED=true}. Two independent flags ⇒ defence in depth.
 *
 * <p>Kept in a separate {@code @Configuration} class so that {@link
 * SpringStandardPluginConfiguration} (the canonical production hook-wiring file) has zero
 * references to test-fixture classes.
 */
@Configuration
@ConditionalOnProperty(name = "zdu.test.framework.enabled", havingValue = "true")
public class ZduTestMutatorConfiguration {

  @Bean
  public AspectMigrationMutator globalTagsV1ToV2Mutator() {
    return new GlobalTagsV1ToV2Mutator();
  }

  @Bean
  public AspectMigrationMutator embedV1ToV2Mutator() {
    return new EmbedV1ToV2Mutator();
  }

  // @Bean  // ZDU SIM: old node — v2→v3 hop missing
  public AspectMigrationMutator embedV2ToV3Mutator() {
    return new EmbedV2ToV3Mutator();
  }

  @Bean
  public AspectMigrationMutator embedV3ToV4Mutator() {
    return new EmbedV3ToV4Mutator();
  }
}
