package com.linkedin.metadata.test;

import static com.linkedin.metadata.test.TestDefinitionParserTest.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.executor.elastic.ElasticTestDefinition;
import com.linkedin.metadata.test.executor.elastic.ElasticTestDefinitionConvertor;
import com.linkedin.metadata.test.executor.elastic.PredicateToFilter;
import com.linkedin.metadata.test.query.TestQuery;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/** Simply tests that test definitions that are supposed to route to Elastic executor work */
public class TestDefinitionExecutorTest {

  private static final TestDefinitionParser PARSER =
      new TestDefinitionParser(PredicateEvaluator.getInstance());
  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:test:test");

  @Test
  public void testParseValidLegacyFormat() throws Exception {
    // No exception when parsing the test
    String jsonTest = loadTest("valid_test_elastic_simple.yaml");
    TestDefinition result = PARSER.deserialize(TEST_URN, jsonTest);
    EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
    EntitySpec mockEntitySpec = Mockito.mock(EntitySpec.class);

    Mockito.when(mockRegistry.getEntitySpec(Mockito.anyString())).thenReturn(mockEntitySpec);

    Mockito.when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(List.of());

    ElasticTestDefinitionConvertor convertor = new ElasticTestDefinitionConvertor(mockRegistry);
    assertTrue(convertor.canEvaluate(result));

    ElasticTestDefinition definition = convertor.convert(result);
    assertEquals(definition.getSelectedEntityTypes().size(), 1);
    assertEquals(definition.getSelectedEntityTypes().get(0), "dataset");
  }

  @Test
  public void testPredicateToFilter() throws Exception {
    // No exception when parsing the test
    String jsonTest = loadTest("valid_test_elastic_conditions.yaml");
    TestDefinition result = PARSER.deserialize(TEST_URN, jsonTest);
    EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
    EntitySpec mockEntitySpec = Mockito.mock(EntitySpec.class);

    Mockito.when(mockRegistry.getEntitySpec(Mockito.anyString())).thenReturn(mockEntitySpec);

    Mockito.when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(List.of());

    Predicate testPredicate = result.getOn().getConditions();
    Filter filter =
        PredicateToFilter.transformPredicateToFilter(
            testPredicate, Mockito.mock(OperationContext.class));
    assertEquals(
        filter.toString(),
        "{or=[{and=[{condition=EQUAL, negated=false, field=platform, value=, values=[urn:li:dataPlatform:teradata]}]}]}");

    ElasticTestDefinitionConvertor convertor = new ElasticTestDefinitionConvertor(mockRegistry);
    assertTrue(convertor.canEvaluate(result));
  }

  @Test
  public void testPredicateToFilterRules() throws Exception {

    List<String> validFiles =
        List.of(
            "valid_test_elastic_conditions_rules.yaml",
            "valid_test_elastic_conditions_rules_tags.yaml");

    for (String file : validFiles) {
      String jsonTest = loadTest(file);
      TestDefinition result = PARSER.deserialize(TEST_URN, jsonTest);
      EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
      EntitySpec mockEntitySpec = Mockito.mock(EntitySpec.class);

      Mockito.when(mockRegistry.getEntitySpec(Mockito.anyString())).thenReturn(mockEntitySpec);

      Map<PathSpec, String> fieldPaths = new HashMap<>();
      fieldPaths.put(new PathSpec("subTypes", "typeNames"), "typeNames");
      Mockito.when(mockEntitySpec.getSearchableFieldPathMap()).thenReturn(fieldPaths);

      ElasticTestDefinitionConvertor convertor = new ElasticTestDefinitionConvertor(mockRegistry);
      assertTrue(convertor.canEvaluate(result));
      ElasticTestDefinition elasticTestDefinition = convertor.convert(result);
      for (String entityType : elasticTestDefinition.getSelectedEntityTypes()) {
        Predicate predicate = elasticTestDefinition.getPassingFilters(entityType);
        assertNotNull(predicate);
        predicate = elasticTestDefinition.getSelectionFilters();
        assertNotNull(predicate);
      }
    }
    //    String expectedPassingFilter = "{or=[{and=[{condition=EQUAL, field=platform,
    // value=urn:li:dataPlatform:teradata}, {condition=EQUAL, field=typeNames, value=table}]}]}";
    //    assert
    // elasticTestDefinition.getPassingFilters("dataset").toString().equals(expectedPassingFilter);
    //    assert
    // elasticTestDefinition.getSelectionFilters("dataset").toString().equals("{or=[{and=[{condition=EQUAL, field=platform, value=urn:li:dataPlatform:teradata}]}]}");
  }

  @Test
  public void testPredicateToFilterRulesRelativeDates() throws Exception {

    List<String> validFiles = List.of("valid_test_relative_timestamp.yaml");

    for (String file : validFiles) {
      String jsonTest = loadTest(file);
      TestDefinition result = PARSER.deserialize(TEST_URN, jsonTest);
      EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
      EntitySpec mockEntitySpec = Mockito.mock(EntitySpec.class);

      Mockito.when(mockRegistry.getEntitySpec(Mockito.anyString())).thenReturn(mockEntitySpec);

      Mockito.when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(List.of());

      ElasticTestDefinitionConvertor convertor = new ElasticTestDefinitionConvertor(mockRegistry);
      assertTrue(convertor.canEvaluate(result));
      ElasticTestDefinition elasticTestDefinition = convertor.convert(result);
      for (String entityType : elasticTestDefinition.getSelectedEntityTypes()) {
        Predicate predicate = elasticTestDefinition.getPassingFilters(entityType);
        assertNotNull(predicate);
        assertEquals(
            ((Predicate)
                    ((Predicate) predicate.getOperands().get().get(0).getExpression())
                        .getOperands()
                        .get()
                        .get(0)
                        .getExpression())
                .operatorType(),
            OperatorType.GREATER_THAN);
        assertEquals(
            ((Predicate)
                    ((Predicate) predicate.getOperands().get().get(0).getExpression())
                        .getOperands()
                        .get()
                        .get(1)
                        .getExpression())
                .operatorType(),
            OperatorType.LESS_THAN);
        Set<TestQuery> testQueries = Predicate.extractQueriesForPredicate(predicate);
        TestQuery testQuery = new TestQuery("operation.lastUpdatedTimestamp");
        assertTrue(testQueries.contains(testQuery));
      }
    }
  }
}
