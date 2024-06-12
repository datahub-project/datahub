package com.linkedin.metadata.test;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.exception.TestDefinitionParsingException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Simply tests that parsing works as expected for different test definition formats. */
public class TestDefinitionParserTest {

  private static final TestDefinitionParser PARSER =
      new TestDefinitionParser(PredicateEvaluator.getInstance());
  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:test:test");

  @Test
  public void testParseValidLegacyFormat() throws Exception {
    // No exception when parsing the test
    String jsonTest = loadTest("valid_legacy_test.yaml");
    TestDefinition result = PARSER.deserialize(TEST_URN, jsonTest);

    String expected =
        "TestDefinition(urn=urn:li:test:test, "
            + "on=TestMatch(entityTypes=[dataset], "
            + "conditions=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=ANY_EQUALS, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=editableDatasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[value1]))]))])), "
            + "rules=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=NOT, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=EXISTS, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=editableDatasetProperties.description))]))]))]), "
            + "actions=TestActions(passing=[], failing=[]), md5=98225469bcd4d1e114e4dbd103e0f182, "
            + "rawDefinition={\"on\":{\"types\":[\"dataset\"],\"match\":[{\"query\":\"editableDatasetProperties"
            + ".description\",\"operation\":\"equals\",\"params\":{\"values\":[\"value1\"]}}]},\"rules\":[{\"query\""
            + ":\"editableDatasetProperties.description\",\"operation\":\"exists\",\"negate\":true}]})";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testParseValidFormatSimple() throws Exception {
    // No exception when parsing the test
    String jsonTest = loadTest("valid_test_simple.yaml");
    TestDefinition result = PARSER.deserialize(TEST_URN, jsonTest);

    String expected =
        "TestDefinition("
            + "urn=urn:li:test:test, "
            + "on=TestMatch(entityTypes=[dataset], "
            + "conditions=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=STARTS_WITH, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.name)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[special_prefix]))]))])), "
            + "rules=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=EXISTS, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=editableDatasetProperties.description))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=CONTAINS_STR, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[pii]))]))]), "
            + "actions=TestActions(passing=[], failing=[]), md5=e1c7bf2eacbb33a65bec057c8b21ceae, rawDefinition="
            + "{\"on\":{\"types\":[\"dataset\"],\"conditions\":[{\"property\":\"datasetProperties.name\",\"operator"
            + "\":\"starts_with\",\"values\":[\"special_prefix\"]}]},\"rules\":{\"and\":[{\"property\":\"editableDat"
            + "asetProperties.description\",\"operator\":\"exists\"},{\"property\":\"datasetProperties.description\",\""
            + "operator\":\"contains_str\",\"values\":[\"pii\"]}]}})";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testParseValidFormatComplex() throws Exception {
    // No exception when parsing the test
    String jsonTest = loadTest("valid_test_complex.yaml");
    TestDefinition result = PARSER.deserialize(TEST_URN, jsonTest);

    String expected =
        "TestDefinition("
            + "urn=urn:li:test:test, "
            + "on=TestMatch(entityTypes=[dataset, chart, dashboard], "
            + "conditions=Predicate(operatorType=NOT, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=STARTS_WITH, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.name)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[special_prefix]))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=REGEX_MATCH, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.name)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[.*exclude.*]))])), Operand(index=2, name=null, "
            + "expression=Predicate(operatorType=OR, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=EXISTS, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=editableDatasetProperties.description))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=NOT, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=STARTS_WITH, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[special_prefix]))]))]))]))]))]))])), "
            + "rules=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=NOT, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=EXISTS, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=editableDatasetProperties.description))]))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=OR, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=CONTAINS_ANY, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=editableDatasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[required field option 1]))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=CONTAINS_ANY, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[required field option 2]))]))])), Operand(index=2, name=null, "
            + "expression=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=OR, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=AND, operands=[Operand(index=0, name=null,"
            + " expression=Predicate(operatorType=NOT, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=CONTAINS_ANY, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[required field option 2]))]))]))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=OR, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=IS_FALSE, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.description))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=IS_TRUE, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.description))]))]))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=CONTAINS_ANY, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[required field option 2]))]))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=NOT, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=CONTAINS_ANY, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[required field option 2]))]))]))]))]))]), "
            + "actions=TestActions(passing=[], failing=[]), md5=baeee89ee84f1fb2a1187feca05deaee, rawDefinition={\"on\""
            + ":{\"types\":[\"dataset\",\"chart\",\"dashboard\"],\"conditions\":{\"not\":[{\"property\":"
            + "\"datasetProperties.name\",\"operator\":\"starts_with\",\"values\":[\"special_prefix\"]},{\"property\""
            + ":\"datasetProperties.name\",\"operator\":\"regex_match\",\"values\":[\".*exclude.*\"]},{\"or\":[{"
            + "\"property\":\"editableDatasetProperties.description\",\"operator\":\"exists\"},{\"and\":[{\"not\":{"
            + "\"property\":\"datasetProperties.description\",\"operator\":\"starts_with\",\"values\":[\"special_prefix"
            + "\"]}}]}]}]}},\"rules\":[{\"not\":{\"property\":\"editableDatasetProperties.description\",\"operator\":"
            + "\"exists\"}},{\"or\":[{\"property\":\"editableDatasetProperties.description\",\"operator\":"
            + "\"contains_any\",\"values\":[\"required field option 1\"]},{\"property\":\"datasetProperties.description"
            + "\",\"operator\":\"contains_any\",\"values\":[\"required field option 2\"]}]},{\"and\":[{\"or\":[{\"and\":"
            + "[{\"not\":[{\"property\":\"datasetProperties.description\",\"operator\":\"contains_any\",\"values\":["
            + "\"required field option 2\"]}]},{\"or\":[{\"property\":\"datasetProperties.description\",\"operator\":"
            + "\"is_false\"},{\"property\":\"datasetProperties.description\",\"operator\":\"is_true\"}]}]},{\"property"
            + "\":\"datasetProperties.description\",\"operator\":\"contains_any\",\"values\":[\"required field option 2"
            + "\"]}]},{\"not\":[{\"property\":\"datasetProperties.description\",\"operator\":\"contains_any\",\"values"
            + "\":[\"required field option 2\"]}]}]}]})";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testParseValidFormatUppercase() throws Exception {
    // No exception when parsing the test
    String jsonTest = loadTest("valid_test_uppercase.yaml");
    TestDefinition result = PARSER.deserialize(TEST_URN, jsonTest);

    String expected =
        "TestDefinition("
            + "urn=urn:li:test:test, "
            + "on=TestMatch(entityTypes=[dataset], "
            + "conditions=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=ANY_EQUALS, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=editableDatasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[value1]))]))])), rules=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=EXISTS, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=editableDatasetProperties.description))]))]), "
            + "actions=TestActions(passing=[], failing=[]), md5=9ec294c89d834ce4ff8eea17205b7798, rawDefinition={\"on\""
            + ":{\"types\":[\"dataset\"],\"conditions\":[{\"property\":\"editableDatasetProperties.description\","
            + "\"operator\":\"EQUALS\",\"values\":[\"value1\"]}]},\"rules\":[{\"query\":\"editableDatasetProperties"
            + ".description\",\"operation\":\"EXISTS\"}]})";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testParseValidActionsTest() throws Exception {
    String jsonTest = loadTest("valid_actions_test.yaml");
    TestDefinition result = PARSER.deserialize(TEST_URN, jsonTest);

    String expected =
        "TestDefinition("
            + "urn=urn:li:test:test, "
            + "on=TestMatch(entityTypes=[dataset], "
            + "conditions=Predicate(operatorType=AND, "
            + "operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=STARTS_WITH, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.name)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[special_prefix]))]))])), "
            + "rules=Predicate(operatorType=AND, operands=[Operand(index=0, name=null, "
            + "expression=Predicate(operatorType=EXISTS, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=editableDatasetProperties.description))])), Operand(index=1, name=null, "
            + "expression=Predicate(operatorType=CONTAINS_STR, operands=[Operand(index=0, name=query, "
            + "expression=Query(query=datasetProperties.description)), Operand(index=1, name=values, "
            + "expression=StringListLiteral(values=[pii]))]))]), "
            + "actions=TestActions(passing=[TestAction(type=ADD_OWNERS, "
            + "params={values=[urn:li:corpuser:1, urn:li:corpGroup:2]}), "
            + "TestAction(type=REMOVE_OWNERS, params={values=[urn:li:corpuser:1, urn:li:corpGroup:2]})], "
            + "failing=[TestAction(type=ADD_GLOSSARY_TERMS, params={values=[urn:li:glossaryTerm:1, urn:li:glossaryTerm:2]}), "
            + "TestAction(type=REMOVE_GLOSSARY_TERMS, params={values=[urn:li:glossaryTerm:1, urn:li:glossaryTerm:2]})]),"
            + " md5=7afe4e94f453a60519e67d14b373e3d9, rawDefinition={\"on\":{\"types\":[\"dataset\"],\"conditions\":"
            + "[{\"property\":\"datasetProperties.name\",\"operator\":\"starts_with\",\"values\":[\"special_prefix\"]}"
            + "]},\"rules\":{\"and\":[{\"property\":\"editableDatasetProperties.description\",\"operator\":\"exists\"},"
            + "{\"property\":\"datasetProperties.description\",\"operator\":\"contains_str\",\"values\":[\"pii\"]}]}"
            + ",\"actions\":{\"passing\":[{\"type\":\"ADD_OWNERS\",\"params\":{\"values\":[\"urn:li:corpuser:1\",\""
            + "urn:li:corpGroup:2\"]}},{\"type\":\"REMOVE_OWNERS\",\"params\":{\"values\":[\"urn:li:corpuser:1\",\""
            + "urn:li:corpGroup:2\"]}}],\"failing\":[{\"type\":\"ADD_GLOSSARY_TERMS\",\"values\":[\""
            + "urn:li:glossaryTerm:1\",\"urn:li:glossaryTerm:2\"]},{\"type\":\"remove_glossary_terms\",\"values\":[\""
            + "urn:li:glossaryTerm:1\",\"urn:li:glossaryTerm:2\"]}]}})";

    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testParseInvalidFormatMissingOn() throws Exception {
    String jsonTest = loadTest("invalid_test_missing_on.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidFormatMissingRules() throws Exception {
    String jsonTest = loadTest("invalid_test_missing_rules.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidFormatMissingValues() throws Exception {
    String jsonTest = loadTest("invalid_test_missing_values.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidFormatUnknownOperator() throws Exception {
    String jsonTest = loadTest("invalid_test_bad_operator.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidFormatBadOr() throws Exception {
    String jsonTest = loadTest("invalid_test_bad_or.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidFormatBadAnd() throws Exception {
    String jsonTest = loadTest("invalid_test_bad_and.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidLegacyFormatMissingParams() throws Exception {
    String jsonTest = loadTest("invalid_legacy_test_missing_params.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidLegacyFormatMissingParamValues() throws Exception {
    String jsonTest = loadTest("invalid_legacy_test_missing_values.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidActionsTestBadActionType() throws Exception {
    String jsonTest = loadTest("invalid_test_bad_action_type.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidActionsTestBadActionParams() throws Exception {
    String jsonTest = loadTest("invalid_test_bad_action_params.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  @Test
  public void testParseInvalidActionsTestBadActionsObject() throws Exception {
    // Passing and failing objects are not arrays (should be)
    String jsonTest = loadTest("invalid_test_bad_actions.yaml");
    Assert.assertThrows(
        TestDefinitionParsingException.class, () -> PARSER.deserialize(TEST_URN, jsonTest));
  }

  public static String loadTest(String resourceName) throws Exception {
    URL url = Resources.getResource(resourceName);
    return convertYamlToJson(Resources.toString(url, StandardCharsets.UTF_8));
  }

  static String convertYamlToJson(String yaml) throws Exception {
    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    yamlReader
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    Object obj = yamlReader.readValue(yaml, Object.class);

    ObjectMapper jsonWriter = new ObjectMapper();
    return jsonWriter.writeValueAsString(obj);
  }
}
