package com.linkedin.gms.factory.plugins;

import static org.testng.Assert.*;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutatorChain;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.config.DataHubConfiguration;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Verifies that concrete {@link AspectMigrationMutator} Spring beans are auto-collected into {@link
 * AspectMigrationMutatorChain} via the {@link SpringStandardPluginConfiguration} wiring.
 */
@SpringBootTest(
    classes = {
      SpringStandardPluginConfiguration.class,
      AspectMigrationMutatorChainInjectionTest.TestMigrationConfig.class
    })
@TestPropertySource(properties = {"metadataChangeProposal.validation.ignoreUnknown=true"})
public class AspectMigrationMutatorChainInjectionTest extends AbstractTestNGSpringContextTests {

  @Autowired private ApplicationContext context;

  @MockitoBean(answers = Answers.RETURNS_MOCKS)
  private ConfigurationProvider configurationProvider;

  @BeforeClass
  private void setup() {
    Mockito.when(configurationProvider.getDatahub()).thenReturn(new DataHubConfiguration());
  }

  @Test
  public void testChain_withRegisteredMutator_isEnabled() {
    AspectMigrationMutatorChain chain = chain();
    assertTrue(chain.isEnabled(), "Chain should be enabled when mutators are registered");
  }

  @Test
  public void testChain_withRegisteredMutator_populatesChainByAspect() {
    AspectMigrationMutatorChain chain = chain();
    assertEquals(chain.getChainByAspect().size(), 1);
    assertTrue(chain.getChainByAspect().containsKey("ownership"));
    assertEquals(chain.getChainByAspect().get("ownership").size(), 1);
  }

  @Test
  public void testChain_mutatorNotDiscoveredAsStandaloneMutationHook() {
    // Concrete mutators must appear only as AspectMigrationMutator beans, never as standalone
    // MutationHook beans. AspectMigrationMutator does not extend MutationHook, so the type
    // system enforces this — the chain is the sole MutationHook in the migration system.
    assertEquals(
        context.getBeansOfType(AspectMigrationMutator.class).size(),
        1,
        "Exactly one AspectMigrationMutator bean expected");
    long migrationChainCount =
        context.getBeansOfType(MutationHook.class).values().stream()
            .filter(b -> b instanceof AspectMigrationMutatorChain)
            .count();
    assertEquals(
        migrationChainCount,
        1,
        "Exactly one AspectMigrationMutatorChain MutationHook bean expected");
  }

  private AspectMigrationMutatorChain chain() {
    MutationHook bean = context.getBean("aspectMigrationMutatorChain", MutationHook.class);
    assertTrue(bean instanceof AspectMigrationMutatorChain);
    return (AspectMigrationMutatorChain) bean;
  }

  /** Registers a single concrete mutator for the ownership aspect (v1 → v2). */
  @Configuration
  static class TestMigrationConfig {

    @Bean
    public AspectMigrationMutator testOwnershipMigration() {
      return new AspectMigrationMutator() {
        @Override
        public String getAspectName() {
          return "ownership";
        }

        @Override
        public long getSourceVersion() {
          return 1L;
        }

        @Override
        public long getTargetVersion() {
          return 2L;
        }

        @Override
        @Nullable
        protected RecordTemplate transform(
            @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
          return sourceAspect;
        }
      };
    }
  }
}
