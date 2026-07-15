package com.linkedin.metadata.postgres;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchService;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchService.PostgresScrollId;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchWriteSink;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.SearchUtil;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.action.explain.ExplainResponse;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for {@link PostgresEntitySearchService} against PostgreSQL, mirroring core
 * keyword search scenarios covered for OpenSearch in {@link
 * com.linkedin.metadata.search.query.SearchDAOTestBase} (search, filter, doc count, raw, scroll).
 */
public class PostgresEntitySearchServiceIT {

  private static final String DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)";

  /** Sorts before {@link #DATASET_URN} when ordering by urn ascending. */
  private static final String DATASET_URN_B = "urn:li:dataset:(urn:li:dataPlatform:hive,db.b,PROD)";

  private static final String DATASET_DOC =
      "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)\","
          + "\"_entityType\":\"dataset\","
          + "\"browsePaths\":[\"/prod/test\"],"
          + "\"_aspects\":{\"datasetProperties\":{"
          + "\"name\":\"PgItName\",\"qualifiedName\":\"q\",\"description\":\"PgItDesc\","
          + "\"_systemmetadata\":{\"version\":7,\"runId\":\"pg_it_run\"}"
          + "}}}";

  private static final String DATASET_DOC_B =
      "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.b,PROD)\","
          + "\"_entityType\":\"dataset\","
          + "\"browsePaths\":[\"/prod/test\"],"
          + "\"_aspects\":{\"datasetProperties\":{"
          + "\"name\":\"PgItName\",\"qualifiedName\":\"qb\",\"description\":\"PgItDesc\","
          + "\"_systemmetadata\":{\"version\":1,\"runId\":\"pg_it_run_b\"}"
          + "}}}";

  private static final String DATASET_URN_C = "urn:li:dataset:(urn:li:dataPlatform:hive,db.c,PROD)";

  private static final String DATASET_DOC_FACET_SCALAR =
      "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)\","
          + "\"_entityType\":\"dataset\","
          + "\"browsePaths\":[\"/prod/test\"],"
          + "\"_aspects\":{\"datasetProperties\":{"
          + "\"name\":\"FacetScalarDoc\",\"qualifiedName\":\"q\",\"description\":\"d\","
          + "\"facetScalar\":\"only\","
          + "\"_systemmetadata\":{\"version\":1,\"runId\":\"facet_scalar\"}"
          + "}}}";

  private static final String DATASET_DOC_FACET_ARRAY =
      "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)\","
          + "\"_entityType\":\"dataset\","
          + "\"browsePaths\":[\"/prod/test\"],"
          + "\"_aspects\":{\"datasetProperties\":{"
          + "\"name\":\"FacetArrayDoc\",\"qualifiedName\":\"q\",\"description\":\"d\","
          + "\"facetTags\":[\"alpha\",\"beta\"],"
          + "\"_systemmetadata\":{\"version\":1,\"runId\":\"facet_arr\"}"
          + "}}}";

  private static final String DATASET_DOC_FACET_ONE_ELEMENT =
      "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)\","
          + "\"_entityType\":\"dataset\","
          + "\"browsePaths\":[\"/prod/test\"],"
          + "\"_aspects\":{\"datasetProperties\":{"
          + "\"name\":\"FacetOneDoc\",\"qualifiedName\":\"q\",\"description\":\"d\","
          + "\"facetOne\":[\"solo\"],"
          + "\"_systemmetadata\":{\"version\":1,\"runId\":\"facet_one\"}"
          + "}}}";

  private static final String DATASET_DOC_SHARED_TAG_A =
      "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)\","
          + "\"_entityType\":\"dataset\","
          + "\"browsePaths\":[\"/prod/test\"],"
          + "\"_aspects\":{\"datasetProperties\":{"
          + "\"name\":\"SharedA\",\"qualifiedName\":\"q\",\"description\":\"d\","
          + "\"facetShared\":[\"urn:li:tag:shared\"],"
          + "\"_systemmetadata\":{\"version\":1,\"runId\":\"shared_a\"}"
          + "}}}";

  private static final String DATASET_DOC_SHARED_TAG_B =
      "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.c,PROD)\","
          + "\"_entityType\":\"dataset\","
          + "\"browsePaths\":[\"/prod/test\"],"
          + "\"_aspects\":{\"datasetProperties\":{"
          + "\"name\":\"SharedB\",\"qualifiedName\":\"qc\",\"description\":\"d\","
          + "\"facetShared\":[\"urn:li:tag:shared\"],"
          + "\"_systemmetadata\":{\"version\":1,\"runId\":\"shared_b\"}"
          + "}}}";

  private Database database;
  private PostgresSqlSetupProperties props;
  private PostgresEntitySearchWriteSink sink;
  private PostgresEntitySearchService searchService;
  private OperationContext opContext;

  @BeforeClass
  public void beforeClass() throws Exception {
    PostgresTestUtils.IntegrationNamespace ns =
        PostgresTestUtils.newIntegrationNamespace("pgsearch_read");
    String schema = ns.getSchema();
    String tablePrefix = ns.getTablePrefix();

    PostgreSQLContainer<?> postgres = PostgresTestUtils.startPostgresWithPgvector();
    props = PostgresTestUtils.testPgSearchProperties(schema, tablePrefix);
    database =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pgsearch_read_it"));
    try (var c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.applyPgSearchEntityTables(c, props);
    }
    sink = new PostgresEntitySearchWriteSink(database, props);
    searchService = new PostgresEntitySearchService(database, props, TEST_SEARCH_SERVICE_CONFIG);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    EbeanTestUtils.shutdownDatabase(database);
  }

  @BeforeMethod
  public void truncate() throws Exception {
    try (var c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.truncateSearchRow(c, props);
    }
  }

  @Test
  public void search_starWildcard_returnsHit_likeElasticsearchMatchAll() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    SearchResult r =
        searchService.search(opContext, List.of("dataset"), "*", null, List.of(), 0, 10, List.of());
    assertEquals(r.getNumEntities(), 1);
    assertEquals(r.getEntities().get(0).getEntity().toString(), DATASET_URN);
  }

  @Test
  public void search_fulltextDisabled_skipsFtsPredicate_likeFilterOnlyQuery() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    OperationContext noFts = opContext.withSearchFlags(f -> f.setFulltext(false));
    SearchResult r =
        searchService.search(
            noFts,
            List.of("dataset"),
            "not_in_tsvector_token_xyz",
            null,
            List.of(),
            0,
            10,
            List.of());
    assertEquals(r.getNumEntities(), 1);
  }

  @Test
  public void search_fullTextMatchesInsertedDocument() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    SearchResult r =
        searchService.search(
            opContext, List.of("dataset"), "PgItName", null, List.of(), 0, 10, List.of());
    assertTrue(r.getNumEntities() >= 1, "expected at least one hit");
    assertFalse(r.getEntities().isEmpty());
    assertEquals(r.getEntities().get(0).getEntity().toString(), DATASET_URN);
    assertTrue(r.getEntities().get(0).getScore() > 0);
  }

  @Test
  public void autoComplete_returnsDistinctTierTextHits() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    AutoCompleteResult ac =
        searchService.autoComplete(opContext, "dataset", "PgIt", null, null, 10);
    assertTrue(ac.getSuggestions().size() >= 1);
    boolean saw = false;
    for (int i = 0; i < ac.getSuggestions().size(); i++) {
      String s = ac.getSuggestions().get(i);
      if (s != null && s.contains("PgIt")) {
        saw = true;
        break;
      }
    }
    assertTrue(saw, "expected tier-1 text to contain document tokens");
  }

  @Test
  public void aggregateByValue_groupsByEntityType_andJsonField() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    Map<String, Long> byType =
        searchService.aggregateByValue(opContext, List.of("dataset"), "_entityType", null, 20);
    assertEquals(byType.get("dataset"), Long.valueOf(1L));

    Map<String, Long> byName =
        searchService.aggregateByValue(
            opContext, List.of("dataset"), "_aspects.datasetProperties.name", null, 20);
    assertEquals(byName.get("PgItName"), Long.valueOf(1L));
  }

  @Test
  public void aggregateByValue_jsonScalar_fieldSingleBucket_likeEsKeyword() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC_FACET_SCALAR, DATASET_URN);
    Map<String, Long> m =
        searchService.aggregateByValue(
            opContext, List.of("dataset"), "_aspects.datasetProperties.facetScalar", null, 20);
    assertEquals(m.get("only"), Long.valueOf(1L));
  }

  @Test
  public void aggregateByValue_jsonArray_fieldPerElementBuckets_likeEsTermsOnArray() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC_FACET_ARRAY, DATASET_URN);
    Map<String, Long> m =
        searchService.aggregateByValue(
            opContext, List.of("dataset"), "_aspects.datasetProperties.facetTags", null, 20);
    assertEquals(m.get("alpha"), Long.valueOf(1L));
    assertEquals(m.get("beta"), Long.valueOf(1L));
  }

  @Test
  public void aggregateByValue_jsonArray_singleElement_oneBucket() {
    sink.upsertDocumentBySearchGroup(
        opContext, "primary", DATASET_DOC_FACET_ONE_ELEMENT, DATASET_URN);
    Map<String, Long> m =
        searchService.aggregateByValue(
            opContext, List.of("dataset"), "_aspects.datasetProperties.facetOne", null, 20);
    assertEquals(m.get("solo"), Long.valueOf(1L));
  }

  @Test
  public void aggregateByValue_jsonArray_twoDocuments_mergeCounts() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC_SHARED_TAG_A, DATASET_URN);
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC_SHARED_TAG_B, DATASET_URN_C);
    Map<String, Long> m =
        searchService.aggregateByValue(
            opContext, List.of("dataset"), "_aspects.datasetProperties.facetShared", null, 20);
    assertEquals(m.get("urn:li:tag:shared"), Long.valueOf(2L));
  }

  @Test
  public void explain_returnsPgSearchIndex_andRank() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    ExplainResponse ex =
        searchService.explain(
            opContext,
            "PgItName",
            DATASET_URN,
            "dataset",
            null,
            List.of(),
            null,
            null,
            10,
            List.of());
    assertEquals(ex.getIndex(), "pgSearch");
    assertEquals(ex.getId(), DATASET_URN);
    assertTrue(ex.isExists());
    assertTrue(ex.getExplanation().getValue().floatValue() >= 0f);
  }

  @Test
  public void filter_numericCompare_onJsonPath_versionGreaterThan() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    Criterion c =
        new Criterion()
            .setField("_aspects.datasetProperties._systemmetadata.version")
            .setCondition(Condition.GREATER_THAN)
            .setValues(new StringArray("5"));
    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    SearchResult r =
        searchService.search(opContext, List.of("dataset"), "*", f, List.of(), 0, 10, List.of());
    assertEquals(r.getNumEntities(), 1);
  }

  @Test
  public void filter_iequal_matchesCaseInsensitiveField() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    Criterion c =
        new Criterion()
            .setField("_aspects.datasetProperties.name")
            .setCondition(Condition.IEQUAL)
            .setValues(new StringArray("pgitname"));
    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    SearchResult r =
        searchService.search(opContext, List.of("dataset"), "*", f, List.of(), 0, 10, List.of());
    assertEquals(r.getNumEntities(), 1);
  }

  @Test
  public void filter_urnIn_matchesEitherDocument() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC_B, DATASET_URN_B);
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    Criterion c =
        new Criterion()
            .setField("urn")
            .setCondition(Condition.IN)
            .setValues(new StringArray(DATASET_URN, DATASET_URN_B));
    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    SearchResult r =
        searchService.search(opContext, List.of("dataset"), "*", f, List.of(), 0, 10, List.of());
    assertEquals(r.getNumEntities(), 2);
  }

  @Test
  public void filter_unmappedIndexName_matchesNoRows() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    Criterion c =
        new Criterion()
            .setField(SearchUtil.ES_INDEX_FIELD)
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray("no_such_opensearch_index_name_ever_zz_12345"));
    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    SearchResult r =
        searchService.search(opContext, List.of("dataset"), "*", f, List.of(), 0, 10, List.of());
    assertEquals(r.getNumEntities(), 0);
  }

  @Test
  public void docCount_matchesElasticsearchStyleDocCount() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    long n = searchService.docCount(opContext, "dataset", null);
    assertEquals(n, 1L);
  }

  @Test
  public void raw_returnsSourceMap_likeElasticSearchRawEntity() throws Exception {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    Urn urn = Urn.createFromString(DATASET_URN);
    Map<Urn, Map<String, Object>> raw = searchService.raw(opContext, Set.of(urn));
    assertEquals(raw.size(), 1);
    Map<String, Object> doc = raw.get(urn);
    assertNotNull(doc);
    assertEquals(doc.get("urn"), DATASET_URN);
    assertEquals(doc.get("_entityType"), "dataset");
  }

  @Test
  public void filter_method_sameAsSearchWithStar_likeElasticSearchFilterApi() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    SearchResult r = searchService.filter(opContext, "dataset", null, List.of(), 0, 10);
    assertEquals(r.getNumEntities(), 1);
    assertEquals(r.getEntities().get(0).getEntity().toString(), DATASET_URN);
  }

  @Test
  public void search_entityTypeFacet_returnsAggregation_likeElasticsearchFacets() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    SearchResult r =
        searchService.search(
            opContext, List.of("dataset"), "*", null, List.of(), 0, 10, List.of("_entityType"));
    assertNotNull(r.getMetadata());
    assertNotNull(r.getMetadata().getAggregations());
    assertFalse(r.getMetadata().getAggregations().isEmpty());
    AggregationMetadata agg = r.getMetadata().getAggregations().get(0);
    assertEquals(agg.getName(), "_entityType");
    assertTrue(agg.getAggregations().containsKey("dataset"));
    assertEquals(agg.getAggregations().get("dataset").longValue(), 1L);
    assertFalse(agg.getFilterValues().isEmpty());
    assertTrue(
        agg.getFilterValues().stream()
            .anyMatch(
                fv ->
                    "dataset".equals(fv.getValue())
                        && fv.getFacetCount() != null
                        && fv.getFacetCount() == 1L));
  }

  @Test
  public void search_offsetPagination_secondPage_matchesElasticsearchFromSize() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC_B, DATASET_URN_B);
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    SearchResult first =
        searchService.search(opContext, List.of("dataset"), "*", null, List.of(), 0, 1, List.of());
    SearchResult second =
        searchService.search(opContext, List.of("dataset"), "*", null, List.of(), 1, 1, List.of());
    assertEquals(first.getEntities().size(), 1);
    assertEquals(second.getEntities().size(), 1);
    assertEquals(first.getEntities().get(0).getEntity().toString(), DATASET_URN_B);
    assertEquals(second.getEntities().get(0).getEntity().toString(), DATASET_URN);
  }

  @Test
  public void browse_groupsNextSegment_likeElasticsearchBrowseDao() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    BrowseResult br = searchService.browse(opContext, "dataset", "/prod", null, 0, 10);
    assertTrue(br.getNumGroups() >= 1);
    assertTrue(
        br.getGroups().stream().anyMatch(g -> "test".equals(g.getName()) && g.getCount() == 1));
  }

  @Test
  public void browseV2_returnsGroups_withFullText_likeBrowseSearchCombo() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    BrowseResultV2 br = searchService.browseV2(opContext, "dataset", "/prod", null, "*", 0, 10);
    assertTrue(br.getNumGroups() >= 1);
  }

  @Test
  public void getBrowsePaths_returnsDocumentBrowsePaths() throws Exception {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    List<String> paths =
        searchService.getBrowsePaths(opContext, "dataset", Urn.createFromString(DATASET_URN));
    assertEquals(paths, List.of("/prod/test"));
  }

  @Test
  public void structuredScroll_secondPageEmpty_likeElasticsearchStructuredScroll() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    ScrollResult first =
        searchService.structuredScroll(
            opContext, List.of("dataset"), "*", null, List.of(), null, null, 1, List.of());
    assertFalse(first.getEntities().isEmpty());
    String next = first.getScrollId();
    assertNotNull(next);
    ScrollResult second =
        searchService.structuredScroll(
            opContext, List.of("dataset"), "*", null, List.of(), next, null, 1, List.of());
    assertTrue(second.getEntities().isEmpty() || second.getScrollId() == null);
  }

  @Test
  public void fullTextScroll_withUrnSort_usesKeysetOnUrn_likeSearchAfterTieBreak() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC_B, DATASET_URN_B);
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    List<SortCriterion> sortByUrn =
        List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING));
    ScrollResult first =
        searchService.fullTextScroll(
            opContext, List.of("dataset"), "PgItName", null, sortByUrn, null, null, 1, List.of());
    assertEquals(first.getEntities().get(0).getEntity().toString(), DATASET_URN_B);
    ScrollResult second =
        searchService.fullTextScroll(
            opContext,
            List.of("dataset"),
            "PgItName",
            null,
            sortByUrn,
            first.getScrollId(),
            null,
            1,
            List.of());
    assertEquals(second.getEntities().get(0).getEntity().toString(), DATASET_URN);
  }

  @Test
  public void filter_entityTypeCriterion_likeSearchDAOTestBase() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    Criterion c = CriterionUtils.buildCriterion("_entityType", Condition.EQUAL, "dataset");
    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    SearchResult r =
        searchService.search(opContext, List.of("dataset"), "*", f, List.of(), 0, 10, List.of());
    assertEquals(r.getNumEntities(), 1);
  }

  @Test
  public void scroll_usesKeysetCursor_likeElasticsearchSearchAfter() {
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, DATASET_URN);
    ScrollResult first =
        searchService.fullTextScroll(
            opContext, List.of("dataset"), "PgItName", null, List.of(), null, null, 1, List.of());
    assertFalse(first.getEntities().isEmpty());
    String next = first.getScrollId();
    assertNotNull(next);
    ScrollResult second =
        searchService.fullTextScroll(
            opContext, List.of("dataset"), "PgItName", null, List.of(), next, null, 1, List.of());
    assertTrue(second.getEntities().isEmpty() || second.getScrollId() == null);
  }

  @Test
  public void scrollId_roundTrip() throws Exception {
    String id = PostgresScrollId.encodeOffset(42);
    assertEquals(PostgresScrollId.decodeOffset(id), 42);
    assertEquals(PostgresScrollId.decodeOffset(null), 0);
  }

  @Test
  public void scrollId_keysetRoundTrip_likeSearchAfterWrapper() {
    String id = PostgresScrollId.encodeKeysetV1(0.42, DATASET_URN);
    var c = PostgresScrollId.decodeKeyset(id);
    assertTrue(c.isPresent());
    assertEquals(c.get().getLastRank(), 0.42, 1e-9);
    assertEquals(c.get().getLastUrn(), DATASET_URN);
    assertFalse(PostgresScrollId.decodeKeyset(null).isPresent());
  }
}
