package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.config.search.StructuredPropertyQueryConfiguration;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.StructuredPropertyUrnIdentity;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StructuredPropertySearchCatalogTest {

  private static final StructuredPropertyQueryConfiguration CONFIG =
      StructuredPropertyQueryConfiguration.builder()
          .cacheTtlSeconds(300)
          .defaultTextWeight(0.8f)
          .stemmedFactor(0.5f)
          .pageSize(25)
          .maxProperties(1000)
          .build();

  private SearchRetriever searchRetriever;
  private CachingAspectRetriever aspectRetriever;
  private OperationContext opContext;
  private EntityRegistry entityRegistry;
  private StructuredPropertySearchCatalog catalog;

  @BeforeMethod
  public void setUp() {
    searchRetriever = mock(SearchRetriever.class);
    aspectRetriever = mock(CachingAspectRetriever.class);
    opContext = mock(OperationContext.class);
    RetrieverContext retrieverContext = mock(RetrieverContext.class);
    entityRegistry = TestOperationContexts.defaultEntityRegistry();
    catalog = new StructuredPropertySearchCatalog(CONFIG);
    when(opContext.getRetrieverContext()).thenReturn(retrieverContext);
    when(retrieverContext.getSearchRetriever()).thenReturn(searchRetriever);
    when(retrieverContext.getCachingAspectRetriever()).thenReturn(aspectRetriever);
  }

  @Test
  public void testSkipsSearchWhenNoTargetEntitySupportsStructuredProperties() {
    EntitySpec structuredPropertySpec =
        entityRegistry.getEntitySpec(STRUCTURED_PROPERTY_ENTITY_NAME);

    assertTrue(catalog.indexed(opContext, List.of(structuredPropertySpec)).isEmpty());
    verify(searchRetriever, never()).scroll(anyList(), any(), isNull(), anyInt(), anyList(), any());
    verify(aspectRetriever, never()).getLatestAspectObjects(any(), anySet(), anySet());
  }

  @Test
  public void testLoadsDefinitionsForMixedTargets() throws Exception {
    Urn hiddenUrn = UrnUtils.getUrn("urn:li:structuredProperty:hiddenLabel");
    Urn numberUrn = UrnUtils.getUrn("urn:li:structuredProperty:priority");
    StructuredPropertyDefinition hidden =
        definition("hiddenLabel", "string")
            .setSearchConfiguration(
                new DataHubSearchConfig()
                    .setQueryByDefault(false)
                    .setCopyTo(new StringArray(List.of("name"))));
    StructuredPropertyDefinition number =
        definition("priority", "number")
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    ScrollResult page =
        new ScrollResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity().setEntity(hiddenUrn),
                    new SearchEntity().setEntity(numberUrn)))
            .setNumEntities(2)
            .setPageSize(2);
    when(searchRetriever.scroll(
            anyList(), isNull(), isNull(), anyInt(), anyList(), any(SearchFlags.class)))
        .thenReturn(page);
    when(aspectRetriever.getLatestAspectObjects(
            eq(opContext), anySet(), eq(Set.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                hiddenUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(new DataMap(hidden.data()))),
                numberUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(new DataMap(number.data())))));

    List<EntitySpec> targets =
        List.of(
            entityRegistry.getEntitySpec(STRUCTURED_PROPERTY_ENTITY_NAME),
            entityRegistry.getEntitySpec(DATASET_ENTITY_NAME));
    List<StructuredPropertySearchCatalog.IndexedProperty> properties =
        catalog.indexed(opContext, targets);

    assertEquals(properties.size(), 1);
    assertEquals(properties.get(0).fieldPath(), "structuredProperties.hiddenLabel");
    assertFalse(properties.get(0).queryByDefault());
    assertEquals(properties.get(0).copyTo(), List.of("name"));
    assertEquals(properties.get(0).textWeight(), 0.8f, 0.0001f);
    assertEquals(properties.get(0).stemmedWeight(), 0.4f, 0.0001f);
    assertEquals(catalog.indexed(opContext, targets), properties);

    ArgumentCaptor<Integer> pageSize = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<List<String>> entities = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<SearchFlags> flags = ArgumentCaptor.forClass(SearchFlags.class);
    verify(searchRetriever, times(1))
        .scroll(
            entities.capture(), isNull(), isNull(), pageSize.capture(), anyList(), flags.capture());
    assertEquals(entities.getValue(), List.of(STRUCTURED_PROPERTY_ENTITY_NAME));
    assertEquals(pageSize.getValue().intValue(), CONFIG.getPageSize());
    assertFalse(flags.getValue().isIncludeSoftDeleted());
    verify(aspectRetriever, times(1))
        .getLatestAspectObjects(
            eq(opContext), anySet(), eq(Set.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)));
  }

  @Test
  public void testStopsAtConfiguredPropertyLimit() throws Exception {
    StructuredPropertyQueryConfiguration limited =
        StructuredPropertyQueryConfiguration.builder()
            .cacheTtlSeconds(300)
            .defaultTextWeight(0.8f)
            .stemmedFactor(0.5f)
            .pageSize(500)
            .maxProperties(1)
            .build();
    StructuredPropertySearchCatalog limitedCatalog = new StructuredPropertySearchCatalog(limited);
    Urn first = UrnUtils.getUrn("urn:li:structuredProperty:firstLabel");
    Urn second = UrnUtils.getUrn("urn:li:structuredProperty:secondLabel");
    ScrollResult page =
        new ScrollResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity().setEntity(first), new SearchEntity().setEntity(second)))
            .setNumEntities(2)
            .setPageSize(2)
            .setScrollId("next");
    when(searchRetriever.scroll(
            anyList(), isNull(), isNull(), eq(1), anyList(), any(SearchFlags.class)))
        .thenReturn(page);
    when(aspectRetriever.getLatestAspectObjects(eq(opContext), eq(Set.of(first)), anySet()))
        .thenReturn(
            Map.of(
                first,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(
                        new DataMap(
                            definition("firstLabel", "string")
                                .setSearchConfiguration(
                                    new DataHubSearchConfig().setQueryByDefault(true))
                                .data())))));

    List<StructuredPropertySearchCatalog.IndexedProperty> properties =
        limitedCatalog.indexed(
            opContext, List.of(entityRegistry.getEntitySpec(DATASET_ENTITY_NAME)));

    assertEquals(properties.size(), 1);
    assertEquals(properties.get(0).fieldPath(), "structuredProperties.firstLabel");
    verify(searchRetriever, times(1))
        .scroll(anyList(), isNull(), isNull(), eq(1), anyList(), any(SearchFlags.class));
  }

  @Test
  public void testSkipsUnreadableDefinitionAndCachesTheRest() throws Exception {
    Urn brokenUrn = UrnUtils.getUrn("urn:li:structuredProperty:brokenLabel");
    Urn keptUrn = UrnUtils.getUrn("urn:li:structuredProperty:keptLabel");
    ScrollResult page =
        new ScrollResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity().setEntity(brokenUrn), new SearchEntity().setEntity(keptUrn)))
            .setNumEntities(2)
            .setPageSize(2);
    when(searchRetriever.scroll(
            anyList(), isNull(), isNull(), anyInt(), anyList(), any(SearchFlags.class)))
        .thenReturn(page);
    when(aspectRetriever.getLatestAspectObjects(
            eq(opContext), anySet(), eq(Set.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                brokenUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME, new Aspect(unreadableDefinition())),
                keptUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(
                        new DataMap(
                            definition("keptLabel", "string")
                                .setSearchConfiguration(
                                    new DataHubSearchConfig().setQueryByDefault(true))
                                .data())))));

    List<EntitySpec> targets = List.of(entityRegistry.getEntitySpec(DATASET_ENTITY_NAME));
    List<StructuredPropertySearchCatalog.IndexedProperty> properties =
        catalog.indexed(opContext, targets);

    assertEquals(properties.size(), 1);
    assertEquals(properties.get(0).fieldPath(), "structuredProperties.keptLabel");
    assertEquals(properties.get(0).textWeight(), 0.8f, 0.0001f);
    assertEquals(properties.get(0).stemmedWeight(), 0.4f, 0.0001f);
    assertEquals(catalog.indexed(opContext, targets), properties);
    verify(searchRetriever, times(1))
        .scroll(anyList(), isNull(), isNull(), anyInt(), anyList(), any(SearchFlags.class));
  }

  @Test
  public void testUrnPropertyFullTextReadsParsedSibling() {
    Urn stewardUrn = UrnUtils.getUrn("urn:li:structuredProperty:steward");
    StructuredPropertyDefinition steward =
        definition("steward", "urn")
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    ScrollResult page =
        new ScrollResult()
            .setEntities(new SearchEntityArray(new SearchEntity().setEntity(stewardUrn)))
            .setNumEntities(1)
            .setPageSize(1);
    when(searchRetriever.scroll(
            anyList(), isNull(), isNull(), anyInt(), anyList(), any(SearchFlags.class)))
        .thenReturn(page);
    when(aspectRetriever.getLatestAspectObjects(
            eq(opContext), anySet(), eq(Set.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                stewardUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(new DataMap(steward.data())))));

    List<StructuredPropertySearchCatalog.IndexedProperty> properties =
        catalog.indexed(opContext, List.of(entityRegistry.getEntitySpec(DATASET_ENTITY_NAME)));

    assertEquals(properties.size(), 1);
    assertEquals(properties.get(0).fieldPath(), "structuredProperties.steward");
    assertEquals(
        properties.get(0).fullTextPath(),
        "structuredProperties.steward" + StructuredPropertyUrnIdentity.FIELD_SUFFIX);
  }

  /**
   * Building the search that loads the catalog reads the catalog again, through template binding
   * and the lifecycle stage filters. The nested read must not start a second load, and must leave
   * the cache usable once the entry expires.
   */
  @Test
  public void testReentrantReadDuringLoad() throws Exception {
    StructuredPropertyQueryConfiguration shortTtl =
        StructuredPropertyQueryConfiguration.builder()
            .cacheTtlSeconds(1)
            .defaultTextWeight(0.8f)
            .stemmedFactor(0.5f)
            .pageSize(17)
            .maxProperties(1000)
            .build();
    StructuredPropertySearchCatalog reentrantCatalog =
        new StructuredPropertySearchCatalog(shortTtl);
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:priority");
    ScrollResult page =
        new ScrollResult()
            .setEntities(new SearchEntityArray(new SearchEntity().setEntity(propertyUrn)))
            .setNumEntities(1)
            .setPageSize(1);
    AtomicInteger scrolls = new AtomicInteger();
    when(searchRetriever.scroll(
            anyList(), isNull(), isNull(), anyInt(), anyList(), any(SearchFlags.class)))
        .thenAnswer(
            invocation -> {
              if (scrolls.getAndIncrement() == 0) {
                reentrantCatalog.catalogStamp(opContext);
              }
              return page;
            });
    when(aspectRetriever.getLatestAspectObjects(
            eq(opContext), anySet(), eq(Set.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                propertyUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(
                        new DataMap(
                            definition("priority", "string")
                                .setSearchConfiguration(
                                    new DataHubSearchConfig().setQueryByDefault(true))
                                .data())))));

    List<EntitySpec> targets = List.of(entityRegistry.getEntitySpec(DATASET_ENTITY_NAME));
    assertEquals(reentrantCatalog.indexed(opContext, targets).size(), 1);
    assertEquals(scrolls.get(), 1);

    Thread.sleep(1_100);

    assertEquals(reentrantCatalog.indexed(opContext, targets).size(), 1);
    assertEquals(scrolls.get(), 2);
  }

  /** valueType is a string, so reading the definition throws instead of returning a type. */
  private static DataMap unreadableDefinition() {
    DataMap definition = new DataMap();
    definition.put("qualifiedName", "brokenLabel");
    definition.put("valueType", "not-a-urn");
    definition.put("entityTypes", new DataList(List.of("urn:li:entityType:datahub.dataset")));
    DataMap search = new DataMap();
    search.put("queryByDefault", true);
    definition.put("searchConfiguration", search);
    return definition;
  }

  private static StructuredPropertyDefinition definition(String name, String type) {
    return new StructuredPropertyDefinition()
        .setQualifiedName(name)
        .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub." + type))
        .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
  }
}
