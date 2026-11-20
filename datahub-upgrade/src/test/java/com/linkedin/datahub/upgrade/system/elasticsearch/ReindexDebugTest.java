package com.linkedin.datahub.upgrade.system.elasticsearch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

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
import com.linkedin.metadata.search.EntitySearchService;
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
import org.testng.annotations.Test;

public class ReindexDebugTest {

  private static final String INDEX = "datasetindex_v2";
  private static final Urn VERSIONED_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:businessName");
  private static final Urn UNVERSIONED_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:retentionClass");

  @Test
  public void testConstructionReadsNoStructuredPropertyDefinitions() {
    // Spring builds this upgrade for every general upgrade run, so building it must not read
    // definitions; only a ReindexDebug run does.
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getStructuredProperties())
        .thenReturn(StructuredPropertiesConfiguration.builder().enabled(true).build());
    AspectDao aspectDao = mock(AspectDao.class);

    ReindexDebug reindexDebug =
        new ReindexDebug(
            mock(SystemMetadataService.class),
            mock(TimeseriesAspectService.class),
            mock(EntitySearchService.class),
            mock(GraphService.class),
            mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class),
            configurationProvider,
            aspectDao);

    assertEquals(reindexDebug.steps().size(), 3);
    verifyNoInteractions(aspectDao);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testForcedReindexUsesVersionedAndUnversionedStructuredPropertiesWhenEnabled()
      throws Exception {
    // A forced rebuild creates the index from the definitions it is given, so any definition
    // missing here leaves that property's values unmapped and unsearchable in the new index.
    ElasticSearchService elasticSearchService = mock(ElasticSearchService.class);
    when(elasticSearchService.getIndexBuilder(anyString())).thenReturn(mock(ESIndexBuilder.class));
    ReindexConfig datasetIndexConfig = mock(ReindexConfig.class);
    when(datasetIndexConfig.name()).thenReturn(INDEX);
    when(elasticSearchService.buildReindexConfigs(any(OperationContext.class), any()))
        .thenReturn(List.of(datasetIndexConfig));

    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getStructuredProperties())
        .thenReturn(StructuredPropertiesConfiguration.builder().enabled(true).build());

    AspectDao aspectDao = mock(AspectDao.class);
    // First call: status aspects (none removed). Second call: property definitions.
    when(aspectDao.streamAspects(anyString(), anyString()))
        .thenReturn(Stream.empty())
        .thenReturn(
            Stream.of(
                definitionAspect(VERSIONED_PROPERTY_URN, "businessName", "10000000000000"),
                definitionAspect(UNVERSIONED_PROPERTY_URN, "retentionClass", null)));

    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    UpgradeContext upgradeContext = mock(UpgradeContext.class);
    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.parsedArgs()).thenReturn(Map.of("index", Optional.of(INDEX)));

    ReindexDebug reindexDebug =
        new ReindexDebug(
            mock(SystemMetadataService.class),
            mock(TimeseriesAspectService.class),
            elasticSearchService,
            mock(GraphService.class),
            mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class),
            configurationProvider,
            aspectDao);
    UpgradeStep reindexStep =
        reindexDebug.steps().stream()
            .filter(step -> "ReindexDebugStep".equals(step.id()))
            .findFirst()
            .orElseThrow();

    assertEquals(
        reindexStep.executable().apply(upgradeContext).result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<Set<Pair<Urn, StructuredPropertyDefinition>>> captor =
        ArgumentCaptor.forClass(Set.class);
    verify(elasticSearchService).buildReindexConfigs(eq(opContext), captor.capture());
    Map<Urn, StructuredPropertyDefinition> passed = new HashMap<>();
    captor.getValue().forEach(p -> passed.put(p.getFirst(), p.getSecond()));
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
}
