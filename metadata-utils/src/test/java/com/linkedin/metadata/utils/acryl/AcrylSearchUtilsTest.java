package com.linkedin.metadata.utils.acryl;

import static org.testng.AssertJUnit.assertEquals;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
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

  @Test
  public void testGreaterThanOrEqualToPredicate() {
    Filter filter =
        RecordUtils.toRecordTemplate(
            Filter.class,
            "{\"or\":[{\"and\":[{\"condition\":\"GREATER_THAN_OR_EQUAL_TO\", \"field\":\"timestamp\", \"value\":\"0\", \"values\":[\"0\"]}]}]}");
    Predicate actual = AcrylSearchUtils.convertFilterToPredicate(filter);
    assertEquals(createGreaterThanOrEqualToPredicate(), actual);
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

    // Wrap the platform predicate in an OR
    Predicate platformORPredicate =
        new Predicate(
            OperatorType.OR, Collections.singletonList(new Operand(0, null, platformPredicate)));

    // Create the ANY_EQUALS predicate for _entityType
    Predicate entityTypePredicate =
        new Predicate(
            OperatorType.ANY_EQUALS,
            Arrays.asList(
                new Operand(0, null, new Query("_entityType")),
                new Operand(1, null, new StringListLiteral(Collections.singletonList("chart")))));

    // Wrap the entityType predicate in an OR
    Predicate entityTypeORPredicate =
        new Predicate(
            OperatorType.OR, Collections.singletonList(new Operand(0, null, entityTypePredicate)));

    // Create the outer AND predicate combining platform and entityType
    Predicate outerAndPredicate =
        new Predicate(
            OperatorType.AND,
            Arrays.asList(
                new Operand(0, null, platformORPredicate),
                new Operand(1, null, entityTypeORPredicate)));

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

    // Create the OR predicates for each condition
    Predicate orPlatformHive =
        new Predicate(
            OperatorType.OR,
            Collections.singletonList(new Operand(0, null, notPlatformHivePredicate)));
    Predicate orEntityType =
        new Predicate(
            OperatorType.OR,
            Collections.singletonList(new Operand(0, null, notEntityTypePredicate)));
    Predicate orOwners =
        new Predicate(
            OperatorType.OR, Collections.singletonList(new Operand(0, null, notOwnersPredicate)));
    Predicate orTags =
        new Predicate(
            OperatorType.OR, Collections.singletonList(new Operand(0, null, notTagsPredicate)));
    Predicate orPlatformFeast =
        new Predicate(
            OperatorType.OR,
            Collections.singletonList(new Operand(0, null, notPlatformFeastPredicate)));

    // Create the outer AND predicate combining all conditions
    Predicate outerAndPredicate =
        new Predicate(
            OperatorType.AND,
            Arrays.asList(
                new Operand(0, null, orPlatformHive),
                new Operand(1, null, orEntityType),
                new Operand(2, null, orOwners),
                new Operand(3, null, orTags),
                new Operand(4, null, orPlatformFeast)));

    // Create the top-level OR predicate
    return new Predicate(
        OperatorType.OR, Collections.singletonList(new Operand(0, null, outerAndPredicate)));
  }

  private Predicate createGreaterThanOrEqualToPredicate() {
    // Create the innermost ANY_EQUALS predicate for timestamp
    Predicate equalsPredicate =
        new Predicate(
            OperatorType.ANY_EQUALS,
            Arrays.asList(
                new Operand(0, null, new Query("timestamp")),
                new Operand(1, null, new StringListLiteral(Collections.singletonList("0")))));

    // Create the innermost GREATER_THAN predicate for timestamp
    Predicate greaterPredicate =
        new Predicate(
            OperatorType.GREATER_THAN,
            Arrays.asList(
                new Operand(0, null, new Query("timestamp")),
                new Operand(1, null, new StringListLiteral(Collections.singletonList("0")))));

    // Wrap the timestamp predicate in an OR
    Predicate platformORPredicate =
        new Predicate(
            OperatorType.OR,
            ImmutableList.of(
                new Operand(0, null, equalsPredicate), new Operand(1, null, greaterPredicate)));

    // Create the outer AND predicate combining platform and entityType
    Predicate outerAndPredicate =
        new Predicate(
            OperatorType.AND, Collections.singletonList(new Operand(0, null, platformORPredicate)));

    // Create the top-level OR predicate
    return new Predicate(
        OperatorType.OR, Collections.singletonList(new Operand(0, null, outerAndPredicate)));
  }
}
