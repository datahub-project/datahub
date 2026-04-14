package com.linkedin.metadata.utils;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.SearchUtil.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SearchUtilTest {

  @Test
  public void testConvertToFilters() throws Exception {
    Map<String, Long> aggregations = new HashMap<>();
    aggregations.put("urn:li:tag:abc", 3L);
    aggregations.put("urn:li:tag:def", 0L);

    Set<String> filteredValues = ImmutableSet.of("urn:li:tag:def");

    List<FilterValue> filters = SearchUtil.convertToFilters(aggregations, filteredValues);

    assertEquals(
        filters.get(0),
        new FilterValue()
            .setFiltered(false)
            .setValue("urn:li:tag:abc")
            .setEntity(Urn.createFromString("urn:li:tag:abc"))
            .setFacetCount(3L));

    assertEquals(
        filters.get(1),
        new FilterValue()
            .setFiltered(true)
            .setValue("urn:li:tag:def")
            .setEntity(Urn.createFromString("urn:li:tag:def"))
            .setFacetCount(0L));
  }

  // --- transformFilterForEntities tests ---

  private IndexConvention mockIndexConvention() {
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.getEntityIndexName("dataset")).thenReturn("datasetindex_v2");
    when(indexConvention.getEntityIndexName("chart")).thenReturn("chartindex_v2");
    when(indexConvention.getEntityIndexName("datajob")).thenReturn("datajobindex_v2");
    when(indexConvention.getEntityIndexName("glossaryterm")).thenReturn("glossarytermindex_v2");
    when(indexConvention.getEntityIndexName("corpuser")).thenReturn("corpuserindex_v2");
    return indexConvention;
  }

  private EntityRegistry mockEntityRegistry() {
    EntityRegistry entityRegistry = mock(EntityRegistry.class);

    EntitySpec datasetSpec = mock(EntitySpec.class);
    when(datasetSpec.getName()).thenReturn("dataset");
    when(entityRegistry.getEntitySpec("dataset")).thenReturn(datasetSpec);

    EntitySpec chartSpec = mock(EntitySpec.class);
    when(chartSpec.getName()).thenReturn("chart");
    when(entityRegistry.getEntitySpec("chart")).thenReturn(chartSpec);

    EntitySpec dataJobSpec = mock(EntitySpec.class);
    when(dataJobSpec.getName()).thenReturn("dataJob");
    when(entityRegistry.getEntitySpec("datajob")).thenReturn(dataJobSpec);

    EntitySpec glossaryTermSpec = mock(EntitySpec.class);
    when(glossaryTermSpec.getName()).thenReturn("glossaryTerm");
    when(entityRegistry.getEntitySpec("glossaryterm")).thenReturn(glossaryTermSpec);

    EntitySpec corpUserSpec = mock(EntitySpec.class);
    when(corpUserSpec.getName()).thenReturn("corpUser");
    when(entityRegistry.getEntitySpec("corpuser")).thenReturn(corpUserSpec);

    // Unknown entity throws
    when(entityRegistry.getEntitySpec("unknownentity"))
        .thenThrow(new IllegalArgumentException("Entity not found"));

    return entityRegistry;
  }

  @Test
  public void testTransformFilterForEntitiesNullFilter() {
    IndexConvention indexConvention = mockIndexConvention();
    Filter result = transformFilterForEntities(null, indexConvention, null);
    assertNull(result);
  }

  @Test
  public void testTransformFilterForEntitiesNoEntityTypeFilter() {
    // Filter without _entityType criterion should pass through unchanged
    IndexConvention indexConvention = mockIndexConvention();
    Criterion tagCriterion = buildCriterion("tags.keyword", Condition.EQUAL, "urn:li:tag:abc");

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(tagCriterion))));

    Filter result = transformFilterForEntities(filter, indexConvention, mockEntityRegistry());

    // Should have exactly 1 conjunctive criterion (no V2/V3 split)
    assertEquals(result.getOr().size(), 1);
    assertEquals(result.getOr().get(0).getAnd().get(0).getField(), "tags.keyword");
  }

  @Test
  public void testTransformFilterForEntitiesDualV2V3WithEntityRegistry() {
    // _entityType filter should produce both V2 (_index) and V3 (_entityType) branches
    IndexConvention indexConvention = mockIndexConvention();
    EntityRegistry entityRegistry = mockEntityRegistry();

    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "dataset");

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion))));

    Filter result = transformFilterForEntities(filter, indexConvention, entityRegistry);

    // Should produce 2 conjunctive criteria: V2 and V3
    assertEquals(result.getOr().size(), 2);

    // V2 branch: _index = datasetindex_v2
    ConjunctiveCriterion v2 = result.getOr().get(0);
    assertEquals(v2.getAnd().size(), 1);
    assertEquals(v2.getAnd().get(0).getField(), ES_INDEX_FIELD);
    assertTrue(v2.getAnd().get(0).getValues().contains("datasetindex_v2"));

    // V3 branch: _entityType = dataset (camelCase from EntityRegistry)
    ConjunctiveCriterion v3 = result.getOr().get(1);
    assertEquals(v3.getAnd().size(), 1);
    assertEquals(v3.getAnd().get(0).getField(), "_entityType");
    assertTrue(v3.getAnd().get(0).getValues().contains("dataset"));
  }

  @Test
  public void testTransformFilterForEntitiesV3CamelCaseWithUnderscore() {
    // data_job → V2: datajobindex_v2, V3: dataJob (camelCase via EntityRegistry)
    IndexConvention indexConvention = mockIndexConvention();
    EntityRegistry entityRegistry = mockEntityRegistry();

    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "data_job");

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion))));

    Filter result = transformFilterForEntities(filter, indexConvention, entityRegistry);

    assertEquals(result.getOr().size(), 2);

    // V2: _index = datajobindex_v2
    assertTrue(result.getOr().get(0).getAnd().get(0).getValues().contains("datajobindex_v2"));

    // V3: _entityType = dataJob (camelCase from EntityRegistry, not "datajob")
    assertEquals(result.getOr().get(1).getAnd().get(0).getField(), "_entityType");
    assertTrue(result.getOr().get(1).getAnd().get(0).getValues().contains("dataJob"));
  }

  @Test
  public void testTransformFilterForEntitiesV3CamelCaseCorpUser() {
    // corp_user → V3 should resolve to "corpUser" not "corpuser"
    IndexConvention indexConvention = mockIndexConvention();
    EntityRegistry entityRegistry = mockEntityRegistry();

    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "corp_user");

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion))));

    Filter result = transformFilterForEntities(filter, indexConvention, entityRegistry);

    assertEquals(result.getOr().size(), 2);
    // V3: _entityType = corpUser (not "corpuser")
    assertTrue(result.getOr().get(1).getAnd().get(0).getValues().contains("corpUser"));
  }

  @Test
  public void testTransformFilterForEntitiesV3FallbackWithoutEntityRegistry() {
    // Without EntityRegistry, V3 should fall back to lowercase
    IndexConvention indexConvention = mockIndexConvention();

    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "glossary_term");

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion))));

    Filter result = transformFilterForEntities(filter, indexConvention, null);

    assertEquals(result.getOr().size(), 2);
    // V3 without registry: falls back to lowercase "glossaryterm"
    assertTrue(result.getOr().get(1).getAnd().get(0).getValues().contains("glossaryterm"));
  }

  @Test
  public void testTransformFilterForEntitiesV3FallbackUnknownEntity() {
    // EntityRegistry throws for unknown entity → falls back to lowercase
    IndexConvention indexConvention = mockIndexConvention();
    EntityRegistry entityRegistry = mockEntityRegistry();

    when(indexConvention.getEntityIndexName("unknownentity")).thenReturn("unknownentityindex_v2");

    Criterion entityTypeCriterion =
        buildCriterion("_entityType", Condition.EQUAL, "unknown_entity");

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion))));

    Filter result = transformFilterForEntities(filter, indexConvention, entityRegistry);

    assertEquals(result.getOr().size(), 2);
    // V3: EntityRegistry doesn't know "unknownentity" → lowercase fallback
    assertTrue(result.getOr().get(1).getAnd().get(0).getValues().contains("unknownentity"));
  }

  @Test
  public void testTransformFilterForEntitiesMixedCriteria() {
    // Filter with both _entityType and non-entity criteria
    IndexConvention indexConvention = mockIndexConvention();
    EntityRegistry entityRegistry = mockEntityRegistry();

    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "chart");
    Criterion tagCriterion = buildCriterion("tags", Condition.EQUAL, "urn:li:tag:important");

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(entityTypeCriterion, tagCriterion))));

    Filter result = transformFilterForEntities(filter, indexConvention, entityRegistry);

    assertEquals(result.getOr().size(), 2);

    // V2 branch: tag criterion + _index criterion
    ConjunctiveCriterion v2 = result.getOr().get(0);
    assertEquals(v2.getAnd().size(), 2);
    boolean hasTagInV2 = v2.getAnd().stream().anyMatch(c -> c.getField().equals("tags"));
    boolean hasIndexInV2 = v2.getAnd().stream().anyMatch(c -> c.getField().equals(ES_INDEX_FIELD));
    assertTrue(hasTagInV2, "V2 branch should preserve non-entity-type criteria");
    assertTrue(hasIndexInV2, "V2 branch should have _index criterion");

    // V3 branch: tag criterion + _entityType criterion
    ConjunctiveCriterion v3 = result.getOr().get(1);
    assertEquals(v3.getAnd().size(), 2);
    boolean hasTagInV3 = v3.getAnd().stream().anyMatch(c -> c.getField().equals("tags"));
    boolean hasEntityTypeInV3 =
        v3.getAnd().stream().anyMatch(c -> c.getField().equals("_entityType"));
    assertTrue(hasTagInV3, "V3 branch should preserve non-entity-type criteria");
    assertTrue(hasEntityTypeInV3, "V3 branch should have _entityType criterion");
  }

  @Test
  public void testTransformFilterForEntitiesMultipleEntityValues() {
    // Single criterion with multiple entity type values
    IndexConvention indexConvention = mockIndexConvention();
    EntityRegistry entityRegistry = mockEntityRegistry();

    Criterion entityTypeCriterion =
        buildCriterion("_entityType", Condition.EQUAL, "dataset", "chart");

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion))));

    Filter result = transformFilterForEntities(filter, indexConvention, entityRegistry);

    assertEquals(result.getOr().size(), 2);

    // V2: both index names
    List<String> v2Values = result.getOr().get(0).getAnd().get(0).getValues();
    assertTrue(v2Values.contains("datasetindex_v2"));
    assertTrue(v2Values.contains("chartindex_v2"));

    // V3: both camelCase names
    List<String> v3Values = result.getOr().get(1).getAnd().get(0).getValues();
    assertTrue(v3Values.contains("dataset"));
    assertTrue(v3Values.contains("chart"));
  }

  @Test
  public void testTransformFilterForEntitiesBackwardCompatibleOverload() {
    // 2-arg overload should behave the same as 3-arg with null EntityRegistry
    IndexConvention indexConvention = mockIndexConvention();

    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "dataset");

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion))));

    Filter resultTwoArg = transformFilterForEntities(filter, indexConvention);

    // Should still produce dual V2/V3 with lowercase fallback
    assertEquals(resultTwoArg.getOr().size(), 2);
    assertEquals(resultTwoArg.getOr().get(0).getAnd().get(0).getField(), ES_INDEX_FIELD);
    assertEquals(resultTwoArg.getOr().get(1).getAnd().get(0).getField(), "_entityType");
    // Without EntityRegistry, V3 uses lowercase
    assertTrue(resultTwoArg.getOr().get(1).getAnd().get(0).getValues().contains("dataset"));
  }

  @Test
  public void testTransformFilterForEntitiesNegatedCriterion() {
    // Negated entity type filter should preserve negation in both branches
    IndexConvention indexConvention = mockIndexConvention();
    EntityRegistry entityRegistry = mockEntityRegistry();

    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "dataset");
    entityTypeCriterion.setNegated(true);

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion))));

    Filter result = transformFilterForEntities(filter, indexConvention, entityRegistry);

    assertEquals(result.getOr().size(), 2);
    assertTrue(result.getOr().get(0).getAnd().get(0).isNegated(), "V2 should preserve negation");
    assertTrue(result.getOr().get(1).getAnd().get(0).isNegated(), "V3 should preserve negation");
  }

  @Test
  public void testTransformFilterForEntitiesMultipleConjunctions() {
    // Multiple OR branches, only some with entity type filters
    IndexConvention indexConvention = mockIndexConvention();
    EntityRegistry entityRegistry = mockEntityRegistry();

    // Branch 1: has entity type filter
    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "dataset");
    ConjunctiveCriterion branch1 =
        new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion));

    // Branch 2: no entity type filter
    Criterion tagCriterion = buildCriterion("tags", Condition.EQUAL, "urn:li:tag:abc");
    ConjunctiveCriterion branch2 =
        new ConjunctiveCriterion().setAnd(new CriterionArray(tagCriterion));

    Filter filter = new Filter().setOr(new ConjunctiveCriterionArray(branch1, branch2));

    Filter result = transformFilterForEntities(filter, indexConvention, entityRegistry);

    // Branch 1 splits into V2 + V3 = 2, Branch 2 passes through = 1, total = 3
    assertEquals(result.getOr().size(), 3);
  }
}
