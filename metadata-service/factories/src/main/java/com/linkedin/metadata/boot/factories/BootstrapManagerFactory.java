package com.linkedin.metadata.boot.factories;

import com.google.common.collect.ImmutableList;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import com.linkedin.metadata.boot.steps.IndexDataPlatformsStep;
import com.linkedin.metadata.boot.steps.MigrateHomePageLinksStep;
import com.linkedin.metadata.boot.steps.RemoveClientIdAspectStep;
import com.linkedin.metadata.boot.steps.RestoreColumnLineageIndices;
import com.linkedin.metadata.boot.steps.RestoreDbtSiblingsIndices;
import com.linkedin.metadata.boot.steps.RestoreFormInfoIndicesStep;
import com.linkedin.metadata.boot.steps.RestoreGlossaryIndices;
import com.linkedin.metadata.boot.steps.WaitForSystemUpdateStep;
import com.linkedin.metadata.config.BootstrapConfigurationSupport;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

@Configuration
@Import({
  EntityServiceFactory.class,
  EntityRegistryFactory.class,
  EntitySearchServiceFactory.class,
})
public class BootstrapManagerFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> _entityService;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry _entityRegistry;

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService _entitySearchService;

  @Autowired
  @Qualifier("dataHubUpgradeKafkaListener")
  private BootstrapDependency _dataHubUpgradeKafkaListener;

  @Autowired private ConfigurationProvider _configurationProvider;

  @Bean(name = "bootstrapManager")
  @Scope("singleton")
  @Nonnull
  protected BootstrapManager createInstance(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext) {
    final int asyncWorkerThreads =
        BootstrapConfigurationSupport.requireAsyncWorkerThreads(_configurationProvider);
    final RestoreGlossaryIndices restoreGlossaryIndicesStep =
        new RestoreGlossaryIndices(_entityService, _entitySearchService, _entityRegistry);
    final IndexDataPlatformsStep indexDataPlatformsStep =
        new IndexDataPlatformsStep(_entityService, _entitySearchService);
    final RestoreDbtSiblingsIndices restoreDbtSiblingsIndices =
        new RestoreDbtSiblingsIndices(_entityService);
    final RemoveClientIdAspectStep removeClientIdAspectStep =
        new RemoveClientIdAspectStep(_entityService);
    final RestoreColumnLineageIndices restoreColumnLineageIndices =
        new RestoreColumnLineageIndices(_entityService);
    final WaitForSystemUpdateStep waitForSystemUpdateStep =
        new WaitForSystemUpdateStep(_dataHubUpgradeKafkaListener, _configurationProvider);
    final RestoreFormInfoIndicesStep restoreFormInfoIndicesStep =
        new RestoreFormInfoIndicesStep(_entityService);
    final MigrateHomePageLinksStep migrateHomePageLinksStep =
        new MigrateHomePageLinksStep(_entityService, _entitySearchService);
    final List<BootstrapStep> finalSteps =
        new ArrayList<>(
            ImmutableList.of(
                waitForSystemUpdateStep,
                restoreGlossaryIndicesStep,
                removeClientIdAspectStep,
                restoreDbtSiblingsIndices,
                indexDataPlatformsStep,
                restoreColumnLineageIndices,
                restoreFormInfoIndicesStep));

    if (_configurationProvider.getFeatureFlags().isShowHomePageRedesign()) {
      finalSteps.add(migrateHomePageLinksStep);
    }

    return new BootstrapManager(finalSteps, asyncWorkerThreads);
  }
}
