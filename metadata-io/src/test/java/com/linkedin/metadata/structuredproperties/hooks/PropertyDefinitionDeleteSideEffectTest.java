package com.linkedin.metadata.structuredproperties.hooks;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_KEY_ASPECT_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.batch.PatchMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import io.datahubproject.metadata.context.RetrieverContext;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PropertyDefinitionDeleteSideEffectTest {
  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(PropertyDefinitionDeleteSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(List.of("DELETE"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName("structuredProperty")
                      .aspectName(STRUCTURED_PROPERTY_KEY_ASPECT_NAME)
                      .build(),
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName("structuredProperty")
                      .aspectName(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
                      .build()))
          .build();

  private static final Urn TEST_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.privacy.retentionTime");
  private static final StructuredPropertyDefinition TEST_PROPERTY_DEFINITION =
      new StructuredPropertyDefinition()
          .setValueType(UrnUtils.getUrn("urn:li:type:datahub.string"))
          .setVersion("00000000000001")
          .setEntityTypes(
              new UrnArray(List.of(UrnUtils.getUrn("urn:li:entityType:datahub.dataset"))))
          .setQualifiedName("io.acryl.privacy.retentionTime");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:postgres,calm-pagoda-323403.jaffle_shop.customers,PROD)");
  private CachingAspectRetriever mockAspectRetriever;
  private SearchRetriever mockSearchRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(CachingAspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_PROPERTY_URN), eq(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)))
        .thenReturn(new Aspect(TEST_PROPERTY_DEFINITION.data()));

    mockSearchRetriever = mock(SearchRetriever.class);
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setPageSize(1);
    scrollResult.setNumEntities(1);
    scrollResult.setEntities(
        new SearchEntityArray(List.of(new SearchEntity().setEntity(TEST_DATASET_URN))));
    when(mockSearchRetriever.scroll(
            eq(List.of("dataset")), eq(expectedFilter()), nullable(String.class), anyInt()))
        .thenReturn(scrollResult);

    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mockSearchRetriever)
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(GraphRetriever.EMPTY)
            .build();
  }

  @Test
  public void testDeletePropertyKey() {
    PropertyDefinitionDeleteSideEffect test = new PropertyDefinitionDeleteSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    List<MCPItem> result =
        test.postMCPSideEffect(
                Set.of(
                    TestMCL.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(TEST_PROPERTY_URN)
                        .entitySpec(TEST_REGISTRY.getEntitySpec("structuredProperty"))
                        .aspectSpec(
                            TEST_REGISTRY
                                .getEntitySpec("structuredProperty")
                                .getAspectSpec(STRUCTURED_PROPERTY_KEY_ASPECT_NAME))
                        .build()),
                retrieverContext)
            .collect(Collectors.toList());

    assertEquals(1, result.size());

    verify(mockAspectRetriever, times(1))
        .getLatestAspectObject(
            eq(TEST_PROPERTY_URN), eq(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME));
    verify(mockSearchRetriever, times(1))
        .scroll(eq(List.of("dataset")), eq(expectedFilter()), nullable(String.class), anyInt());

    JsonPatch expectedPatch =
        Json.createPatchBuilder().remove("/properties/" + TEST_PROPERTY_URN).build();
    assertEquals(((PatchMCP) result.get(0)).getPatch(), expectedPatch);
  }

  @Test
  public void testDeletePropertyDefinition() {
    PropertyDefinitionDeleteSideEffect test = new PropertyDefinitionDeleteSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);

    List<MCPItem> result =
        test.postMCPSideEffect(
                Set.of(
                    TestMCL.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(TEST_PROPERTY_URN)
                        .entitySpec(TEST_REGISTRY.getEntitySpec("structuredProperty"))
                        .aspectSpec(
                            TEST_REGISTRY
                                .getEntitySpec("structuredProperty")
                                .getAspectSpec(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))
                        .previousRecordTemplate(TEST_PROPERTY_DEFINITION)
                        .build()),
                retrieverContext)
            .collect(Collectors.toList());

    assertEquals(1, result.size());

    verify(mockAspectRetriever, times(0)).getLatestAspectObject(any(), any());
    verify(mockAspectRetriever, times(0)).getLatestAspectObjects(any(), any());
    verify(mockSearchRetriever, times(1))
        .scroll(eq(List.of("dataset")), eq(expectedFilter()), nullable(String.class), anyInt());

    JsonPatch expectedPatch =
        Json.createPatchBuilder().remove("/properties/" + TEST_PROPERTY_URN).build();
    assertEquals(((PatchMCP) result.get(0)).getPatch(), expectedPatch);
  }

  private static Filter expectedFilter() {
    Filter propertyFilter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    final Criterion propertyExistsCriterion =
        buildExistsCriterion(
            "structuredProperties._versioned.io_acryl_privacy_retentionTime.00000000000001.string");

    andCriterion.add(propertyExistsCriterion);
    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    propertyFilter.setOr(disjunction);

    return propertyFilter;
  }
}
