package com.linkedin.metadata.utils.acryl;

import static org.testng.AssertJUnit.assertEquals;

import com.datahub.util.RecordUtils;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.utils.elasticsearch.AcrylSearchUtils;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

public class AcrylSearchUtilsTest {

  @Test
  public void testCreatePredicate() throws Exception {
    Filter filter =
        RecordUtils.toRecordTemplate(
            Filter.class,
            "{\"or\":[{\"and\":[{\"condition\":\"EQUAL\", \"field\":\"platform\", \"value\":\"urn:li:dataPlatform:looker\", \"values\":[\"urn:li:dataPlatform:looker\"]}, {\"condition\":\"EQUAL\", \"field\":\"_entityType\", \"value\":\"chart\", \"values\":[\"chart\"]}]}]}");
    Predicate predicate = createPredicate();
    Predicate predicate1 = AcrylSearchUtils.convertFilterToPredicate(filter);
    assertEquals(predicate1, predicate);
  }

  private Predicate createPredicate() {
    // Create the innermost ANY_EQUALS predicate for platform
    Predicate platformPredicate =
        new Predicate(
            OperatorType.ANY_EQUALS,
            Arrays.asList(
                new Operand(0, null, new Query("platform")),
                new Operand(
                    1,
                    null,
                    new StringListLiteral(
                        Collections.singletonList("urn:li:dataPlatform:looker")))));

    // Wrap the platform predicate in an AND
    Predicate platformAndPredicate =
        new Predicate(
            OperatorType.AND, Collections.singletonList(new Operand(0, null, platformPredicate)));

    // Create the ANY_EQUALS predicate for _entityType
    Predicate entityTypePredicate =
        new Predicate(
            OperatorType.ANY_EQUALS,
            Arrays.asList(
                new Operand(0, null, new Query("_entityType")),
                new Operand(1, null, new StringListLiteral(Collections.singletonList("chart")))));

    // Wrap the entityType predicate in an AND
    Predicate entityTypeAndPredicate =
        new Predicate(
            OperatorType.AND, Collections.singletonList(new Operand(0, null, entityTypePredicate)));

    // Create the outer AND predicate combining platform and entityType
    Predicate outerAndPredicate =
        new Predicate(
            OperatorType.AND,
            Arrays.asList(
                new Operand(0, null, platformAndPredicate),
                new Operand(1, null, entityTypeAndPredicate)));

    // Create the top-level OR predicate
    return new Predicate(
        OperatorType.OR, Collections.singletonList(new Operand(0, null, outerAndPredicate)));
  }
}
