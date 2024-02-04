package com.linkedin.metadata.test;

import static com.linkedin.metadata.test.TestDefinitionParserTest.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.executor.elastic.ElasticTestDefinition;
import com.linkedin.metadata.test.executor.elastic.ElasticTestDefinitionConvertor;
import com.linkedin.metadata.test.executor.elastic.PredicateToFilter;
import java.util.List;
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
    assert convertor.canEvaluate(result);

    ElasticTestDefinition definition = convertor.convert(result);
    assert definition.getSelectedEntityTypes().size() == 1;
    assert definition.getSelectedEntityTypes().get(0).equals("dataset");
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
    Filter filter = PredicateToFilter.transformPredicateToFilter(testPredicate);
    assert filter
        .toString()
        .equals(
            "{or=[{and=[{condition=EQUAL, field=platform, value=urn:li:dataPlatform:teradata}]}]}");
    System.out.println("Hello World");

    ElasticTestDefinitionConvertor convertor = new ElasticTestDefinitionConvertor(mockRegistry);
    assert convertor.canEvaluate(result);
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

      Mockito.when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(List.of());

      ElasticTestDefinitionConvertor convertor = new ElasticTestDefinitionConvertor(mockRegistry);
      assert convertor.canEvaluate(result);
      ElasticTestDefinition elasticTestDefinition = convertor.convert(result);
      for (String entityType : elasticTestDefinition.getSelectedEntityTypes()) {
        Filter filter = elasticTestDefinition.getPassingFilters(entityType);
        assert filter != null;
        filter = elasticTestDefinition.getSelectionFilters(entityType);
        assert filter != null;
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
      assert convertor.canEvaluate(result);
      ElasticTestDefinition elasticTestDefinition = convertor.convert(result);
      long millis = System.currentTimeMillis();
      long millis7daysago = millis - 7 * 24 * 60 * 60 * 1000;
      long millis6daysago = millis - 6 * 24 * 60 * 60 * 1000;
      for (String entityType : elasticTestDefinition.getSelectedEntityTypes()) {
        Filter filter = elasticTestDefinition.getPassingFilters(entityType);
        assert filter != null;
        assert filter.getOr().get(0).getAnd().get(0).getCondition().equals("GREATER_THAN");
        long computedMillis = Long.parseLong(filter.getOr().get(0).getAnd().get(0).getValue());
        assert computedMillis < millis6daysago;
        assert computedMillis > millis7daysago; // computedMillis is between 6 and 7 days ago
        assert filter.getOr().get(0).getAnd().get(1).getField().equals("lastOperationTime");
      }
    }
  }
}
