package com.linkedin.datahub.graphql.types;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.types.file.DataHubFileType;
import com.linkedin.datahub.graphql.types.template.PageTemplateType;
import com.linkedin.datahub.graphql.util.AspectUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.file.BucketStorageLocation;
import com.linkedin.file.DataHubFileInfo;
import com.linkedin.file.FileUploadScenario;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.template.DataHubPageTemplateProperties;
import com.linkedin.template.DataHubPageTemplateRowArray;
import com.linkedin.template.DataHubPageTemplateSurface;
import com.linkedin.template.DataHubPageTemplateVisibility;
import com.linkedin.template.PageTemplateScope;
import com.linkedin.template.PageTemplateSurfaceType;
import graphql.execution.DataFetcherResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.annotations.Test;

/**
 * DataHubFileMapper and PageTemplateMapper map an entity to null when their required aspect is
 * absent. Both are non-null GraphQL fields, so a selection that only asks for {@code @noAspects}
 * fields (e.g. {@code createDataHubFile { file { urn } }}) must still fetch that aspect — otherwise
 * the mapper returns null and the whole operation fails with a non-nullable field error.
 */
public class RequiredAspectLoaderTest {

  private static final String FILE_URN = "urn:li:dataHubFile:test_file";
  private static final String TEMPLATE_URN = "urn:li:dataHubPageTemplate:test_template";

  /** Request-scoped context whose accumulated selection is urn-only (no aspects required). */
  private static final class UrnOnlySelectionContext implements QueryContext {
    private final ConcurrentHashMap<String, AspectLoadContext> contexts = new ConcurrentHashMap<>();
    private final QueryContext delegate;
    private final com.linkedin.metadata.config.DataHubAppConfiguration appConfigOverride;

    UrnOnlySelectionContext(QueryContext delegate, String entityTypeName) {
      this(delegate, entityTypeName, null);
    }

    UrnOnlySelectionContext(
        QueryContext delegate,
        String entityTypeName,
        com.linkedin.metadata.config.DataHubAppConfiguration appConfigOverride) {
      this.delegate = delegate;
      this.appConfigOverride = appConfigOverride;
      this.contexts.put(entityTypeName, AspectLoadContext.of(Set.of()));
    }

    @Override
    public boolean isAuthenticated() {
      return delegate.isAuthenticated();
    }

    @Override
    public com.datahub.authentication.Authentication getAuthentication() {
      return delegate.getAuthentication();
    }

    @Override
    public com.datahub.plugins.auth.authorization.Authorizer getAuthorizer() {
      return delegate.getAuthorizer();
    }

    @Override
    public io.datahubproject.metadata.context.OperationContext getOperationContext() {
      return delegate.getOperationContext();
    }

    @Override
    public com.linkedin.metadata.config.DataHubAppConfiguration getDataHubAppConfig() {
      return appConfigOverride != null ? appConfigOverride : delegate.getDataHubAppConfig();
    }

    @Override
    public int getMaxParentDepth() {
      return delegate.getMaxParentDepth();
    }

    @Override
    public AspectMappingRegistry getAspectMappingRegistry() {
      return null;
    }

    @Override
    public void setAspectMappingRegistry(AspectMappingRegistry aspectMappingRegistry) {}

    @Override
    public void mergeAspectLoadContext(String entityTypeName, AspectLoadContext loadContext) {
      contexts.merge(entityTypeName, loadContext, AspectLoadContext::union);
    }

    @Override
    public AspectLoadContext getAspectLoadContext(String entityTypeName) {
      return contexts.get(entityTypeName);
    }
  }

  @Test
  public void testDataHubFileHydratesRequiredInfoForUrnOnlySelection() throws Exception {
    Urn urn = UrnUtils.getUrn(FILE_URN);
    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.batchGetV2(any(), eq(Constants.DATAHUB_FILE_ENTITY_NAME), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<String> requested = (Set<String>) invocation.getArgument(3);
              Map<String, EnvelopedAspect> aspects = new HashMap<>();
              if (requested.contains(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)) {
                aspects.put(
                    Constants.DATAHUB_FILE_INFO_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(fileInfo().data())));
              }
              return Map.of(
                  urn,
                  new EntityResponse()
                      .setEntityName(Constants.DATAHUB_FILE_ENTITY_NAME)
                      .setUrn(urn)
                      .setAspects(new EnvelopedAspectMap(aspects)));
            });

    QueryContext context = new UrnOnlySelectionContext(getMockAllowContext(), "DataHubFile");
    List<DataFetcherResult<com.linkedin.datahub.graphql.generated.DataHubFile>> results =
        new DataHubFileType(entityClient).batchLoad(List.of(FILE_URN), context);

    assertNotNull(
        results.get(0).getData(),
        "urn-only selection must still fetch dataHubFileInfo; the mapper nulls the entity without"
            + " it and DataHubFile is non-nullable");
  }

  @Test
  public void testPageTemplateHydratesRequiredPropertiesForUrnOnlySelection() throws Exception {
    Urn urn = UrnUtils.getUrn(TEMPLATE_URN);
    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.batchGetV2(
            any(), eq(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<String> requested = (Set<String>) invocation.getArgument(3);
              Map<String, EnvelopedAspect> aspects = new HashMap<>();
              if (requested.contains(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME)) {
                aspects.put(
                    Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(templateProperties().data())));
              }
              return Map.of(
                  urn,
                  new EntityResponse()
                      .setEntityName(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME)
                      .setUrn(urn)
                      .setAspects(new EnvelopedAspectMap(aspects)));
            });

    QueryContext context =
        new UrnOnlySelectionContext(getMockAllowContext(), "DataHubPageTemplate");
    List<DataFetcherResult<com.linkedin.datahub.graphql.generated.DataHubPageTemplate>> results =
        new PageTemplateType(entityClient).batchLoad(List.of(TEMPLATE_URN), context);

    assertNotNull(
        results.get(0).getData(),
        "urn-only selection must still fetch dataHubPageTemplateProperties; the mapper nulls the"
            + " entity without it");
  }

  /**
   * Mechanism guard for every entry in the central required-aspect table: a urn-only selection must
   * still yield an optimized set containing the mapper-required aspects. This is what protects the
   * Test / Incident / DataContract / DataHubConnection loaders (whose mappers null or throw on a
   * missing aspect) without each needing a bespoke behavioral test.
   */
  @Test
  public void testMapperRequiredAspectsFoldedIntoOptimizedSet() {
    for (String typeName :
        List.of(
            "DataHubFile",
            "DataHubPageTemplate",
            "Test",
            "Incident",
            "DataContract",
            "DataHubConnection",
            "Document")) {
      Set<String> required = AspectUtils.getMapperRequiredAspects(typeName);
      assertFalse(required.isEmpty(), typeName + " should have mapper-required aspects registered");

      QueryContext context = new UrnOnlySelectionContext(getMockAllowContext(), typeName);
      Set<String> optimized =
          AspectUtils.getOptimizedAspects(context, typeName, Set.of("someDefault"), "someKey");

      assertTrue(
          optimized.containsAll(required),
          typeName + " urn-only selection must still fetch " + required + ", got " + optimized);
    }
  }

  /** With the kill switch off, hydration must revert to the full default aspect set. */
  @Test
  public void testOptimizationDisabledFallsBackToDefaults() {
    FeatureFlags flags = new FeatureFlags();
    flags.setGraphQLAspectOptimizationEnabled(false);
    DataHubAppConfiguration appConfig = new DataHubAppConfiguration();
    appConfig.setFeatureFlags(flags);

    QueryContext context = new UrnOnlySelectionContext(getMockAllowContext(), "Dataset", appConfig);
    Set<String> defaults = Set.of("datasetKey", "ownership", "globalTags");

    Set<String> resolved =
        AspectUtils.getOptimizedAspects(context, "Dataset", defaults, "datasetKey");

    assertEquals(
        resolved, defaults, "disabled optimization must return the full default aspect set");
  }

  private static DataHubFileInfo fileInfo() {
    return new DataHubFileInfo()
        .setBucketStorageLocation(
            new BucketStorageLocation().setStorageBucket("bucket").setStorageKey("key"))
        .setOriginalFileName("file.txt")
        .setMimeType("text/plain")
        .setSizeInBytes(10L)
        .setScenario(FileUploadScenario.ASSET_DOCUMENTATION);
  }

  private static DataHubPageTemplateProperties templateProperties() {
    return new DataHubPageTemplateProperties()
        .setRows(new DataHubPageTemplateRowArray())
        .setSurface(
            new DataHubPageTemplateSurface().setSurfaceType(PageTemplateSurfaceType.HOME_PAGE))
        .setVisibility(new DataHubPageTemplateVisibility().setScope(PageTemplateScope.PERSONAL));
  }
}
