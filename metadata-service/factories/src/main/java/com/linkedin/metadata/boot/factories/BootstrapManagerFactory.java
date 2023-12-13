package com.linkedin.metadata.boot.factories;

import com.google.common.collect.ImmutableList;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import com.linkedin.metadata.boot.steps.BackfillBrowsePathsV2Step;
import com.linkedin.metadata.boot.steps.IndexDataPlatformsStep;
import com.linkedin.metadata.boot.steps.IngestDataPlatformInstancesStep;
import com.linkedin.metadata.boot.steps.IngestDataPlatformsStep;
import com.linkedin.metadata.boot.steps.IngestDefaultGlobalSettingsStep;
import com.linkedin.metadata.boot.steps.IngestOwnershipTypesStep;
import com.linkedin.metadata.boot.steps.IngestPoliciesStep;
import com.linkedin.metadata.boot.steps.IngestRetentionPoliciesStep;
import com.linkedin.metadata.boot.steps.IngestRolesStep;
import com.linkedin.metadata.boot.steps.IngestRootUserStep;
import com.linkedin.metadata.boot.steps.RemoveClientIdAspectStep;
import com.linkedin.metadata.boot.steps.RestoreColumnLineageIndices;
import com.linkedin.metadata.boot.steps.RestoreDbtSiblingsIndices;
import com.linkedin.metadata.boot.steps.RestoreGlossaryIndices;
import com.linkedin.metadata.boot.steps.UpgradeDefaultBrowsePathsStep;
import com.linkedin.metadata.boot.steps.WaitForSystemUpdateStep;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.Resource;

@Configuration
@Import({
  EntityServiceFactory.class,
  EntityRegistryFactory.class,
  EntitySearchServiceFactory.class,
  SearchDocumentTransformerFactory.class
})
public class BootstrapManagerFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry _entityRegistry;

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService _entitySearchService;

  @Autowired
  @Qualifier("searchService")
  private SearchService _searchService;

  @Autowired
  @Qualifier("searchDocumentTransformer")
  private SearchDocumentTransformer _searchDocumentTransformer;

  @Autowired
  @Qualifier("entityAspectMigrationsDao")
  private AspectMigrationsDao _migrationsDao;

  @Autowired
  @Qualifier("ingestRetentionPoliciesStep")
  private IngestRetentionPoliciesStep _ingestRetentionPoliciesStep;

  @Autowired
  @Qualifier("dataHubUpgradeKafkaListener")
  private BootstrapDependency _dataHubUpgradeKafkaListener;

  @Autowired private ConfigurationProvider _configurationProvider;

  @Value("${bootstrap.upgradeDefaultBrowsePaths.enabled}")
  private Boolean _upgradeDefaultBrowsePathsEnabled;

  @Value("${bootstrap.backfillBrowsePathsV2.enabled}")
  private Boolean _backfillBrowsePathsV2Enabled;

  @Value("${bootstrap.policies.file}")
  private Resource _policiesResource;

  @Value("${bootstrap.ownershipTypes.file}")
  private Resource _ownershipTypesResource;

  @Bean(name = "bootstrapManager")
  @Scope("singleton")
  @Nonnull
  protected BootstrapManager createInstance() {
    final IngestRootUserStep ingestRootUserStep = new IngestRootUserStep(_entityService);
    final IngestPoliciesStep ingestPoliciesStep =
        new IngestPoliciesStep(
            _entityRegistry,
            _entityService,
            _entitySearchService,
            _searchDocumentTransformer,
            _policiesResource);
    final IngestRolesStep ingestRolesStep = new IngestRolesStep(_entityService, _entityRegistry);
    final IngestDataPlatformsStep ingestDataPlatformsStep =
        new IngestDataPlatformsStep(_entityService);
    final IngestDataPlatformInstancesStep ingestDataPlatformInstancesStep =
        new IngestDataPlatformInstancesStep(_entityService, _migrationsDao);
    final RestoreGlossaryIndices restoreGlossaryIndicesStep =
        new RestoreGlossaryIndices(_entityService, _entitySearchService, _entityRegistry);
    final IndexDataPlatformsStep indexDataPlatformsStep =
        new IndexDataPlatformsStep(_entityService, _entitySearchService, _entityRegistry);
    final RestoreDbtSiblingsIndices restoreDbtSiblingsIndices =
        new RestoreDbtSiblingsIndices(_entityService, _entityRegistry);
    final RemoveClientIdAspectStep removeClientIdAspectStep =
        new RemoveClientIdAspectStep(_entityService);
    final RestoreColumnLineageIndices restoreColumnLineageIndices =
        new RestoreColumnLineageIndices(_entityService, _entityRegistry);
    final IngestDefaultGlobalSettingsStep ingestSettingsStep =
        new IngestDefaultGlobalSettingsStep(_entityService);
    final WaitForSystemUpdateStep waitForSystemUpdateStep =
        new WaitForSystemUpdateStep(_dataHubUpgradeKafkaListener, _configurationProvider);
    final IngestOwnershipTypesStep ingestOwnershipTypesStep =
        new IngestOwnershipTypesStep(_entityService, _ownershipTypesResource);

    final List<BootstrapStep> finalSteps =
        new ArrayList<>(
            ImmutableList.of(
                waitForSystemUpdateStep,
                ingestRootUserStep,
                ingestPoliciesStep,
                ingestRolesStep,
                ingestDataPlatformsStep,
                ingestDataPlatformInstancesStep,
                _ingestRetentionPoliciesStep,
                ingestOwnershipTypesStep,
                ingestSettingsStep,
                restoreGlossaryIndicesStep,
                removeClientIdAspectStep,
                restoreDbtSiblingsIndices,
                indexDataPlatformsStep,
                restoreColumnLineageIndices));

    if (_upgradeDefaultBrowsePathsEnabled) {
      finalSteps.add(new UpgradeDefaultBrowsePathsStep(_entityService));
    }

    if (_backfillBrowsePathsV2Enabled) {
      finalSteps.add(new BackfillBrowsePathsV2Step(_entityService, _searchService));
    }

    return new BootstrapManager(finalSteps);
  }
}
