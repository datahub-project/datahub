package com.linkedin.metadata.boot.factories;

import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.incident.IncidentServiceFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.metadata.aspect.hooks.ExtendedModelStructuredPropertyMutator;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import com.linkedin.metadata.boot.steps.IndexDataPlatformsStep;
import com.linkedin.metadata.boot.steps.IngestDataPlatformInstancesStep;
import com.linkedin.metadata.boot.steps.IngestDefaultGlobalSettingsStep;
import com.linkedin.metadata.boot.steps.IngestDefaultPersonasAndViews;
import com.linkedin.metadata.boot.steps.IngestDefaultTagsStep;
import com.linkedin.metadata.boot.steps.IngestEntityTypesStep;
import com.linkedin.metadata.boot.steps.IngestMetadataTestsStep;
import com.linkedin.metadata.boot.steps.IngestPoliciesStep;
import com.linkedin.metadata.boot.steps.IngestRetentionPoliciesStep;
import com.linkedin.metadata.boot.steps.IngestStructuredPropertyExtensionsStep;
import com.linkedin.metadata.boot.steps.IngestionMetadataTestResultsActionStep;
import com.linkedin.metadata.boot.steps.MigrateAssertionsSummaryStep;
import com.linkedin.metadata.boot.steps.MigrateFreshnessAssertionCronToSinceTheLastCheck;
import com.linkedin.metadata.boot.steps.MigrateIncidentsSummaryStep;
import com.linkedin.metadata.boot.steps.RemoveClientIdAspectStep;
import com.linkedin.metadata.boot.steps.RestoreColumnLineageIndices;
import com.linkedin.metadata.boot.steps.RestoreDbtSiblingsIndices;
import com.linkedin.metadata.boot.steps.RestoreFormInfoIndicesStep;
import com.linkedin.metadata.boot.steps.RestoreGlossaryIndices;
import com.linkedin.metadata.boot.steps.WaitForSystemUpdateStep;
import com.linkedin.metadata.config.ForwardingActionConfiguration;
import com.linkedin.metadata.config.structuredProperties.extensions.ModelExtensionValidationConfiguration;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.IncidentService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
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
  SearchDocumentTransformerFactory.class,
  AssertionServiceFactory.class,
  IncidentServiceFactory.class
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
  @Qualifier("searchService")
  private SearchService _searchService;

  @Autowired
  @Qualifier("searchDocumentTransformer")
  private SearchDocumentTransformer _searchDocumentTransformer;

  @Autowired
<<<<<<< HEAD
  @Qualifier("assertionService")
  private AssertionService _assertionService;

  @Autowired
  @Qualifier("incidentService")
  private IncidentService _incidentService;

  @Autowired
  @Qualifier("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Autowired
=======
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
  @Qualifier("entityAspectDao")
  private AspectMigrationsDao _migrationsDao;

  @Autowired
  @Qualifier("ingestRetentionPoliciesStep")
  private IngestRetentionPoliciesStep _ingestRetentionPoliciesStep;

  @Autowired
  @Qualifier("dataHubUpgradeKafkaListener")
  private BootstrapDependency _dataHubUpgradeKafkaListener;

  @Autowired private ConfigurationProvider _configurationProvider;

  @Autowired private IntegrationsService integrationsService;

  @Value("${bootstrap.defaultPersonas.enabled}")
  private Boolean _defaultPersonasEnabled;

  // Saas-only
  @Autowired
  @Qualifier("ingestMetadataTestsStep")
  private IngestMetadataTestsStep _ingestMetadataTestsStep;

  @Autowired private ApplicationContext applicationContext;

  @Value("${bootstrap.policies.file}")
  private Resource _policiesResource;

  @Bean(name = "bootstrapManager")
  @Scope("singleton")
  @Nonnull
  protected BootstrapManager createInstance(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext) {
    final IngestPoliciesStep ingestPoliciesStep =
        new IngestPoliciesStep(
            _entityService, _entitySearchService, _searchDocumentTransformer, _policiesResource);
    final IngestDataPlatformInstancesStep ingestDataPlatformInstancesStep =
        new IngestDataPlatformInstancesStep(_entityService, _migrationsDao);
    final RestoreGlossaryIndices restoreGlossaryIndicesStep =
        new RestoreGlossaryIndices(_entityService, _entitySearchService);
    final IndexDataPlatformsStep indexDataPlatformsStep =
        new IndexDataPlatformsStep(_entityService, _entitySearchService);
    final RestoreDbtSiblingsIndices restoreDbtSiblingsIndices =
        new RestoreDbtSiblingsIndices(_entityService);
    final RemoveClientIdAspectStep removeClientIdAspectStep =
        new RemoveClientIdAspectStep(_entityService);
    final RestoreColumnLineageIndices restoreColumnLineageIndices =
        new RestoreColumnLineageIndices(_entityService);
    final IngestDefaultGlobalSettingsStep ingestSettingsStep =
        new IngestDefaultGlobalSettingsStep(_entityService);
    final WaitForSystemUpdateStep waitForSystemUpdateStep =
        new WaitForSystemUpdateStep(_dataHubUpgradeKafkaListener, _configurationProvider);
    final IngestEntityTypesStep ingestEntityTypesStep = new IngestEntityTypesStep(_entityService);
    final RestoreFormInfoIndicesStep restoreFormInfoIndicesStep =
        new RestoreFormInfoIndicesStep(_entityService);
    final IngestDefaultTagsStep ingestDefaultTagsStep = new IngestDefaultTagsStep(_entityService);

    final MigrateAssertionsSummaryStep assertionsSummaryStep =
        new MigrateAssertionsSummaryStep(
            _entityService,
            _entitySearchService,
            _assertionService,
            _timeseriesAspectService,
            _configurationProvider);
    final MigrateIncidentsSummaryStep incidentsSummaryStep =
        new MigrateIncidentsSummaryStep(_entityService, _entitySearchService, _incidentService);
    final MigrateFreshnessAssertionCronToSinceTheLastCheck
        migrateFreshnessAssertionCronToSinceTheLastCheckStep =
            new MigrateFreshnessAssertionCronToSinceTheLastCheck(
                _entityService, _searchService, _assertionService);

    final List<BootstrapStep> finalSteps =
        Stream.of(
                waitForSystemUpdateStep,
                ingestPoliciesStep,
                ingestDataPlatformInstancesStep,
                _ingestRetentionPoliciesStep,
                ingestSettingsStep,
                restoreGlossaryIndicesStep,
                removeClientIdAspectStep,
                restoreDbtSiblingsIndices,
                indexDataPlatformsStep,
                restoreColumnLineageIndices,
                ingestEntityTypesStep,
                restoreFormInfoIndicesStep,
                ingestEntityTypesStep,
                assertionsSummaryStep,
                incidentsSummaryStep,
                migrateFreshnessAssertionCronToSinceTheLastCheckStep,
                // Saas-only
                _ingestMetadataTestsStep,
                ingestDefaultTagsStep)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (_defaultPersonasEnabled) {
      finalSteps.add(new IngestDefaultPersonasAndViews(_entityService, _entityRegistry));
    }

    ModelExtensionValidationConfiguration mcpExtensionValidationConfig =
        _configurationProvider.getMetadataChangeProposal().getValidation().getExtensions();
    if (mcpExtensionValidationConfig.isEnabled()) {
      ExtendedModelStructuredPropertyMutator mutator =
          applicationContext.getBean(ExtendedModelStructuredPropertyMutator.class);
      finalSteps.add(
          new IngestStructuredPropertyExtensionsStep(
              _entityService, mutator.getStructuredPropertyMappings()));
    }

    ForwardingActionConfiguration forwardingActionConfiguration =
        _configurationProvider.getMetadataTests().getForwardingAction();
    if (forwardingActionConfiguration.isEnabled()) {
      finalSteps.add(
          new IngestionMetadataTestResultsActionStep(
              _entityService, integrationsService, forwardingActionConfiguration));
    }

    return new BootstrapManager(finalSteps);
  }
}
