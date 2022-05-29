package com.linkedin.metadata.boot.factories;

import com.google.common.collect.ImmutableList;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.boot.steps.IngestDataPlatformInstancesStep;
import com.linkedin.metadata.boot.steps.IngestDataPlatformsStep;
import com.linkedin.metadata.boot.steps.IngestPoliciesStep;
import com.linkedin.metadata.boot.steps.IngestRetentionPoliciesStep;
import com.linkedin.metadata.boot.steps.IngestRootUserStep;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import javax.annotation.Nonnull;


@Configuration
@Import({EntityServiceFactory.class, EntityRegistryFactory.class, EntitySearchServiceFactory.class,
    SearchDocumentTransformerFactory.class})
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
  @Qualifier("searchDocumentTransformer")
  private SearchDocumentTransformer _searchDocumentTransformer;

  @Autowired
  @Qualifier("entityAspectMigrationsDao")
  private AspectMigrationsDao _migrationsDao;

  @Autowired
  @Qualifier("ingestRetentionPoliciesStep")
  private IngestRetentionPoliciesStep _ingestRetentionPoliciesStep;

  @Bean(name = "bootstrapManager")
  @Scope("singleton")
  @Nonnull
  protected BootstrapManager createInstance() {
    final IngestRootUserStep ingestRootUserStep = new IngestRootUserStep(_entityService);
    final IngestPoliciesStep ingestPoliciesStep =
        new IngestPoliciesStep(_entityRegistry, _entityService, _entitySearchService, _searchDocumentTransformer);
    final IngestDataPlatformsStep ingestDataPlatformsStep = new IngestDataPlatformsStep(_entityService);
    final IngestDataPlatformInstancesStep ingestDataPlatformInstancesStep =
        new IngestDataPlatformInstancesStep(_entityService, _migrationsDao);
    return new BootstrapManager(ImmutableList.of(ingestRootUserStep, ingestPoliciesStep, ingestDataPlatformsStep,
        ingestDataPlatformInstancesStep, _ingestRetentionPoliciesStep));
  }
}
