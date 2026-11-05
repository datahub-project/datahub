package com.linkedin.datahub.upgrade.system.elasticsearch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReindexDebugTest {

  private static final String INDEX = "datasetindex_v2";
  private static final Urn VERSIONED_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:logicalDatasetBusinessName");
  private static final Urn UNVERSIONED_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:retentionClass");

  private ElasticSearchService elasticSearchService;
  private ESIndexBuilder indexBuilder;
  private ConfigurationProvider configurationProvider;
  private StructuredPropertiesConfiguration structuredPropertiesConfiguration;
  private AspectDao aspectDao;
  private UpgradeContext upgradeContext;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() throws Exception {
    elasticSearchService = mock(ElasticSearchService.class);
    indexBuilder = mock(ESIndexBuilder.class);
    when(elasticSearchService.getIndexBuilder(anyString())).thenReturn(indexBuilder);

    ReindexConfig datasetIndexConfig = mock(ReindexConfig.class);
    when(datasetIndexConfig.name()).thenReturn(INDEX);
    when(elasticSearchService.buildReindexConfigs(any(OperationContext.class), any()))
        .thenReturn(List.of(datasetIndexConfig));

    structuredPropertiesConfiguration = mock(StructuredPropertiesConfiguration.class);
    configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getStructuredProperties())
        .thenReturn(structuredPropertiesConfiguration);

    aspectDao = mock(AspectDao.class);

    opContext = TestOperationContexts.systemContextNoValidate();
    upgradeContext = mock(UpgradeContext.class);
    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.parsedArgs()).thenReturn(Map.of("index", Optional.of(INDEX)));
  }

  @Test
  public void testForcedReindexUsesVersionedAndUnversionedStructuredPropertiesWhenEnabled()
      throws Exception {
    when(structuredPropertiesConfiguration.isSystemUpdateEnabled()).thenReturn(true);

    EntityAspect versioned =
        definitionAspect(VERSIONED_PROPERTY_URN, "logicalDatasetBusinessName", "10000000000000");
    EntityAspect unversioned = definitionAspect(UNVERSIONED_PROPERTY_URN, "retentionClass", null);
    // First call: status aspects (none removed). Second call: property definitions.
    when(aspectDao.streamAspects(anyString(), anyString()))
        .thenReturn(Stream.empty())
        .thenReturn(Stream.of(versioned, unversioned));

    assertEquals(runReindexDebugStep(), DataHubUpgradeState.SUCCEEDED);

    Map<Urn, StructuredPropertyDefinition> passed = new HashMap<>();
    capturedStructuredProperties().forEach(p -> passed.put(p.getFirst(), p.getSecond()));
    assertEquals(passed.keySet(), Set.of(VERSIONED_PROPERTY_URN, UNVERSIONED_PROPERTY_URN));
    assertEquals(passed.get(VERSIONED_PROPERTY_URN).getVersion(), "10000000000000");
    assertNull(passed.get(UNVERSIONED_PROPERTY_URN).getVersion(GetMode.NULL));
  }

  private static EntityAspect definitionAspect(
      Urn urn, String qualifiedName, @Nullable String version) {
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setQualifiedName(qualifiedName);
    if (version != null) {
      definition.setVersion(version);
    }
    definition.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    definition.setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    EntityAspect aspect = mock(EntityAspect.class);
    when(aspect.getUrn()).thenReturn(urn.toString());
    when(aspect.getMetadata()).thenReturn(RecordUtils.toJsonString(definition));
    return aspect;
  }

  @Test
  public void testForcedReindexSkipsStructuredPropertiesWhenSystemUpdateDisabled()
      throws Exception {
    when(structuredPropertiesConfiguration.isSystemUpdateEnabled()).thenReturn(false);

    assertEquals(runReindexDebugStep(), DataHubUpgradeState.SUCCEEDED);

    assertTrue(capturedStructuredProperties().isEmpty());
    verify(aspectDao, never()).streamAspects(anyString(), anyString());
  }

  private DataHubUpgradeState runReindexDebugStep() {
    ReindexDebug upgrade =
        new ReindexDebug(
            mock(SystemMetadataService.class),
            mock(TimeseriesAspectService.class),
            elasticSearchService,
            mock(GraphService.class),
            mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class),
            configurationProvider,
            aspectDao);
    UpgradeStep reindexStep =
        upgrade.steps().stream()
            .filter(step -> "ReindexDebugStep".equals(step.id()))
            .findFirst()
            .orElseThrow();
    return reindexStep.executable().apply(upgradeContext).result();
  }

  @SuppressWarnings("unchecked")
  private Set<Pair<Urn, StructuredPropertyDefinition>> capturedStructuredProperties()
      throws Exception {
    ArgumentCaptor<Set<Pair<Urn, StructuredPropertyDefinition>>> captor =
        ArgumentCaptor.forClass(Set.class);
    verify(elasticSearchService).buildReindexConfigs(eq(opContext), captor.capture());
    return captor.getValue();
  }
}
