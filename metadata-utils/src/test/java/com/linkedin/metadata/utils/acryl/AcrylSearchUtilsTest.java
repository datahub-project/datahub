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
    assertEquals(predicate, predicate1);
  }

  @Test
  public void testComplexInvertedFilter() throws Exception {
    Filter filter =
        RecordUtils.toRecordTemplate(
            Filter.class,
            "{\"or\": [\n"
                + "  {\n"
                + "    \"and\": [\n"
                + "      {\n"
                + "        \"field\": \"platform\",\n"
                + "        \"values\": [\n"
                + "          \"urn:li:dataPlatform:hive\"\n"
                + "        ],\n"
                + "        \"condition\": \"EQUAL\",\n"
                + "        \"negated\": true\n"
                + "      },\n"
                + "      {\n"
                + "        \"field\": \"_entityType\",\n"
                + "        \"values\": [\n"
                + "          \"dataset\"\n"
                + "        ],\n"
                + "        \"condition\": \"EQUAL\",\n"
                + "        \"negated\": true\n"
                + "      },\n"
                + "      {\n"
                + "        \"field\": \"owners\",\n"
                + "        \"values\": [\n"
                + "          \"urn:li:corpuser:datahub\",\n"
                + "          \"urn:li:corpGroup:jdoe\",\n"
                + "          \"urn:li:corpGroup:bfoo\"\n"
                + "        ],\n"
                + "        \"condition\": \"EQUAL\",\n"
                + "        \"negated\": true\n"
                + "      },\n"
                + "      {\n"
                + "        \"field\": \"tags\",\n"
                + "        \"values\": [\n"
                + "          \"urn:li:tag:Legacy\"\n"
                + "        ],\n"
                + "        \"condition\": \"EQUAL\",\n"
                + "        \"negated\": true\n"
                + "      },\n"
                + "      {\n"
                + "        \"field\": \"platform\",\n"
                + "        \"values\": [\n"
                + "          \"urn:li:dataPlatform:feast\"\n"
                + "        ],\n"
                + "        \"condition\": \"EQUAL\",\n"
                + "        \"negated\": true\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "]}");

    Predicate actual = AcrylSearchUtils.convertFilterToPredicate(filter);
    assertEquals(createInvertedPredicate(), actual);
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

  public static Predicate createInvertedPredicate() {
    // Create the ANY_EQUALS predicate for platform (Hive)
    Predicate platformHivePredicate =
        new Predicate(
            OperatorType.ANY_EQUALS,
            Arrays.asList(
                new Operand(0, null, new Query("platform")),
                new Operand(
                    1,
                    null,
                    new StringListLiteral(Collections.singletonList("urn:li:dataPlatform:hive")))));

    // Wrap the platform predicate in a NOT
    Predicate notPlatformHivePredicate =
        new Predicate(
            OperatorType.NOT,
            Collections.singletonList(new Operand(0, null, platformHivePredicate)));

    // Create the ANY_EQUALS predicate for _entityType
    Predicate entityTypePredicate =
        new Predicate(
            OperatorType.ANY_EQUALS,
            Arrays.asList(
                new Operand(0, null, new Query("_entityType")),
                new Operand(1, null, new StringListLiteral(Collections.singletonList("dataset")))));

    // Wrap the entityType predicate in a NOT
    Predicate notEntityTypePredicate =
        new Predicate(
            OperatorType.NOT, Collections.singletonList(new Operand(0, null, entityTypePredicate)));

    // Create the ANY_EQUALS predicate for owners
    Predicate ownersPredicate =
        new Predicate(
            OperatorType.ANY_EQUALS,
            Arrays.asList(
                new Operand(0, null, new Query("owners")),
                new Operand(
                    1,
                    null,
                    new StringListLiteral(
                        Arrays.asList(
                            "urn:li:corpuser:datahub",
                            "urn:li:corpGroup:jdoe",
                            "urn:li:corpGroup:bfoo")))));

    // Wrap the owners predicate in a NOT
    Predicate notOwnersPredicate =
        new Predicate(
            OperatorType.NOT, Collections.singletonList(new Operand(0, null, ownersPredicate)));

    // Create the ANY_EQUALS predicate for tags
    Predicate tagsPredicate =
        new Predicate(
            OperatorType.ANY_EQUALS,
            Arrays.asList(
                new Operand(0, null, new Query("tags")),
                new Operand(
                    1,
                    null,
                    new StringListLiteral(Collections.singletonList("urn:li:tag:Legacy")))));

    // Wrap the tags predicate in a NOT
    Predicate notTagsPredicate =
        new Predicate(
            OperatorType.NOT, Collections.singletonList(new Operand(0, null, tagsPredicate)));

    // Create the ANY_EQUALS predicate for platform (Feast)
    Predicate platformFeastPredicate =
        new Predicate(
            OperatorType.ANY_EQUALS,
            Arrays.asList(
                new Operand(0, null, new Query("platform")),
                new Operand(
                    1,
                    null,
                    new StringListLiteral(
                        Collections.singletonList("urn:li:dataPlatform:feast")))));

    // Wrap the platform (Feast) predicate in a NOT
    Predicate notPlatformFeastPredicate =
        new Predicate(
            OperatorType.NOT,
            Collections.singletonList(new Operand(0, null, platformFeastPredicate)));

    // Create the AND predicates for each condition
    Predicate andPlatformHive =
        new Predicate(
            OperatorType.AND,
            Collections.singletonList(new Operand(0, null, notPlatformHivePredicate)));
    Predicate andEntityType =
        new Predicate(
            OperatorType.AND,
            Collections.singletonList(new Operand(0, null, notEntityTypePredicate)));
    Predicate andOwners =
        new Predicate(
            OperatorType.AND, Collections.singletonList(new Operand(0, null, notOwnersPredicate)));
    Predicate andTags =
        new Predicate(
            OperatorType.AND, Collections.singletonList(new Operand(0, null, notTagsPredicate)));
    Predicate andPlatformFeast =
        new Predicate(
            OperatorType.AND,
            Collections.singletonList(new Operand(0, null, notPlatformFeastPredicate)));

    // Create the outer AND predicate combining all conditions
    Predicate outerAndPredicate =
        new Predicate(
            OperatorType.AND,
            Arrays.asList(
                new Operand(0, null, andPlatformHive),
                new Operand(1, null, andEntityType),
                new Operand(2, null, andOwners),
                new Operand(3, null, andTags),
                new Operand(4, null, andPlatformFeast)));

    // Create the top-level OR predicate
    return new Predicate(
        OperatorType.OR, Collections.singletonList(new Operand(0, null, outerAndPredicate)));
  }
}
