package com.linkedin.metadata.search.postgres.query;

import static com.linkedin.metadata.utils.SearchUtil.ES_INDEX_FIELD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.postgres.query.PostgresSearchFilterSqlBuilder.SqlFragment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link PostgresSearchFilterSqlBuilder} (parity with ES filter compilation paths).
 */
public class PostgresSearchFilterSqlBuilderTest {

  private static final String ALIAS = "t";

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void build_nullFilter_returnsTrue() {
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(null, opContext, ALIAS);
    assertEquals(f.getSql(), "TRUE");
    assertTrue(f.getArgs().isEmpty());
  }

  @Test
  public void build_emptyOr_returnsTrue() {
    SqlFragment f =
        PostgresSearchFilterSqlBuilder.build(
            new Filter().setOr(new ConjunctiveCriterionArray()), opContext, ALIAS);
    assertEquals(f.getSql(), "TRUE");
  }

  @Test
  public void entityType_equal_sqlUsesColumnBinding() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("_entityType")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray("dataset"))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("entity_type = ?"));
    assertEquals(f.getArgs().size(), 1);
    assertEquals(f.getArgs().get(0), "dataset");
  }

  @Test
  public void urn_equal_sqlUsesColumnBinding() {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)";
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("urn")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray(urn))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("urn = ?"));
    assertEquals(f.getArgs().get(0), urn);
  }

  @Test
  public void jsonField_equal_usesDocumentPath() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("_aspects.datasetProperties.name")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray("hello"))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("document #>>"));
    assertTrue(f.getSql().contains("'datasetProperties'"));
    assertEquals(f.getArgs().get(0), "hello");
  }

  @Test
  public void contain_usesILikeBinding() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("description")
                                    .setCondition(Condition.CONTAIN)
                                    .setValues(new StringArray("needle"))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("ILIKE ?"));
    assertEquals(f.getArgs().get(0), "%needle%");
  }

  @Test
  public void negatedCriterion_wrapsNot() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("_entityType")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray("dataset"))
                                    .setNegated(true)))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("NOT ("));
  }

  @Test
  public void negatedJsonField_equal_usesIsDistinctFrom() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("state")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray("UNPUBLISHED"))
                                    .setNegated(true)))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("IS DISTINCT FROM"));
    assertFalse(f.getSql().contains("NOT ("));
    assertEquals(f.getArgs().get(0), "UNPUBLISHED");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void descendantsInclusion_throwsUnsupported() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("browsePaths")
                                    .setCondition(Condition.DESCENDANTS_INCL)
                                    .setValues(
                                        new StringArray(
                                            "urn:li:dataset:(urn:li:dataPlatform:hive,x,PROD)"))))));
    PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
  }

  @Test
  public void normalizeFieldForJson_stripsKeywordSuffix() {
    assertEquals(
        PostgresSearchFilterSqlBuilder.normalizeFieldForJson("tags.keyword"),
        PostgresSearchFilterSqlBuilder.normalizeFieldForJson("tags"));
  }

  @Test
  public void documentTextPathExpr_escapesQuotesInSegments() {
    String expr = PostgresSearchFilterSqlBuilder.documentTextPathExpr(ALIAS, "a.b'.c");
    assertTrue(expr.startsWith(ALIAS + ".document #>> "));
    assertTrue(expr.contains("''"));
  }

  @Test
  public void resolveSearchGroup_mapsDatasetIndexToPrimarySearchGroup() {
    EntityRegistry reg = opContext.getEntityRegistry();
    EntitySpec dataset = reg.getEntitySpec("dataset");
    String indexName = opContext.getSearchContext().getIndexConvention().getIndexName(dataset);
    Optional<String> sg = PostgresSearchFilterSqlBuilder.resolveSearchGroup(opContext, indexName);
    assertEquals(sg.orElseThrow(), "primary");
  }

  @Test
  public void indexCriterion_mapsToSearchGroupBinding() {
    EntityRegistry reg = opContext.getEntityRegistry();
    EntitySpec dataset = reg.getEntitySpec("dataset");
    String indexName = opContext.getSearchContext().getIndexConvention().getIndexName(dataset);
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField(ES_INDEX_FIELD)
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray(indexName))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("search_group"));
    assertFalse(f.getArgs().isEmpty());
    assertEquals(f.getArgs().get(0), "primary");
  }

  @Test
  public void entityType_in_sqlUsesInClause() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("_entityType")
                                    .setCondition(Condition.IN)
                                    .setValues(new StringArray("dataset", "chart"))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("entity_type IN"));
    assertEquals(f.getArgs().size(), 2);
  }

  @Test
  public void iequal_usesLowerOnDocumentPath() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("_aspects.datasetProperties.name")
                                    .setCondition(Condition.IEQUAL)
                                    .setValues(new StringArray("Hello"))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("LOWER(COALESCE("));
    assertEquals(f.getArgs().get(0), "hello");
  }

  @Test
  public void numericGreaterThan_castsDocumentPathToNumeric() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("_aspects.datasetProperties.customMetric")
                                    .setCondition(Condition.GREATER_THAN)
                                    .setValues(new StringArray("3.5"))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains(")::numeric > ?"));
    assertEquals(f.getArgs().get(0), 3.5);
  }

  @Test
  public void numericLessThanOrEqual_castsDocumentPathToNumeric() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("tier")
                                    .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
                                    .setValues(new StringArray("2"))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains(")::numeric <= ?"));
    assertEquals(f.getArgs().get(0), 2.0);
  }

  @Test
  public void urn_equal_emptyValues_doesNotThrow() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("urn")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray())))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("urn = ?"));
    assertEquals(f.getArgs().size(), 1);
  }

  @Test
  public void urn_in_bindsUrnColumnInList() {
    String u1 = "urn:li:dataset:(urn:li:dataPlatform:hive,a,PROD)";
    String u2 = "urn:li:dataset:(urn:li:dataPlatform:hive,b,PROD)";
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("urn")
                                    .setCondition(Condition.IN)
                                    .setValues(new StringArray(u1, u2))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("t.urn IN ("));
    assertEquals(f.getArgs().get(0), u1);
    assertEquals(f.getArgs().get(1), u2);
  }

  @Test
  public void indexCriterion_unmappedIndex_usesNoSuchSearchGroupSentinel() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField(ES_INDEX_FIELD)
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray("no_such_entity_index_zz_999"))))));
    SqlFragment f = PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS);
    assertTrue(f.getSql().contains("search_group IN ("));
    assertTrue(f.getArgs().contains("__no_such_search_group__"));
  }

  @Test
  public void logicalField_equal_resolvesThroughRegistryToAspectPath() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("sourceType")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray("SYSTEM"))))));
    SqlFragment f =
        PostgresSearchFilterSqlBuilder.build(
            filter, opContext, ALIAS, List.of("dataHubIngestionSource"));
    assertTrue(f.getSql().contains("'dataHubIngestionSourceInfo'"));
    assertTrue(f.getSql().contains("'sourceType'"));
    assertTrue(f.getSql().contains("_aspects"));
  }

  @Test
  public void fieldNameAlias_resolvesToAspectPath() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("_entityName")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray("bob"))))));
    SqlFragment f =
        PostgresSearchFilterSqlBuilder.build(filter, opContext, ALIAS, List.of("dataset"));
    assertTrue(f.getSql().contains("'datasetProperties'"));
    assertTrue(f.getSql().contains("'name'"));
  }

  @Test
  public void negatedEqual_onRegistryResolvedField_usesIsDistinctFromWithAspectPath() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("sourceType")
                                    .setNegated(true)
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray("SYSTEM"))))));
    SqlFragment f =
        PostgresSearchFilterSqlBuilder.build(
            filter, opContext, ALIAS, List.of("dataHubIngestionSource"));
    assertTrue(f.getSql().contains("IS DISTINCT FROM"));
    assertTrue(f.getSql().contains("dataHubIngestionSourceInfo"));
  }

  @Test
  public void documentJsonbPathExpr_usesJsonbArrowOperator() {
    assertEquals(
        PostgresSearchFilterSqlBuilder.documentJsonbPathExpr(ALIAS, "a.b.c"),
        "t.document #> ARRAY['a','b','c']::text[]");
  }
}
