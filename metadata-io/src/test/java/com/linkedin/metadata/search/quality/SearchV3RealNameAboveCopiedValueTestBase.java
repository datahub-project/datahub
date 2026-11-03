package com.linkedin.metadata.search.quality;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingMappingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingSettingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.SearchWriteAccess;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.UpdateIndicesV3Strategy;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateBootstrap;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRegistry;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFixtures;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLoader;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateSurfaces;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schemafield.SchemaFieldInfo;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import com.linkedin.test.metadata.aspect.batch.TestSystemAspect;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * A catalog where many assets share a display title, a structured property whose values are copied
 * into the name. A user searching for a real name must find that asset first, above every asset
 * whose copied title merely equals it, and every exact match must rank above every match that is
 * not exact.
 *
 * <p>The tranches, best first: EXACT_NAME, EXACT_COPIED, EXACT_COLUMN, EXACT_COLUMN_COPIED, then
 * PARTIAL_NAME (a dataset whose own name shares a substring with the search), PARTIAL_COPIED (only
 * its title does), PARTIAL_COLUMN, PARTIAL_COLUMN_COPIED, then OTHER (stem-only or unrelated).
 * Exact means the whole value equals the search, ignoring case: kennel_booking is exact for
 * kennel_booking, never for kennel_booking_archive or booking. Partial means at least one
 * separator-delimited substring of the search starts one of the value, or the value starts with the
 * search; a partial real name ranks above a partial title, and partial matches are otherwise not
 * ordered among themselves. For "my cat", the asset named "my cat" ranks first; "my cat frank",
 * "awesome my cat" and "cat" are all partial real names in any order. Separators are space,
 * underscore, dot and slash; a hyphen is not one, and there is no stemming. A value that differs
 * from the search only in separators may rank either way and is left out of the comparison. An
 * asset takes its best tranche, graded on its real name first. A record ID typed as the search is
 * checked on its own: that asset first.
 *
 * <p>Every scenario runs through the class-order router set as the search-box default in
 * configuration, with no per-request selection, as a deployment sets it. It runs on two indices:
 * one where the display title is copied into the name and the template infers copied values, and
 * one where it is not copied and the template reads the property's own field. Queries are typed
 * unquoted, and only the tranche order is asserted, never the order inside a tranche.
 */
public abstract class SearchV3RealNameAboveCopiedValueTestBase
    extends AbstractTestNGSpringContextTests {
  private static final long TIME = 1700000000000L;
  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:datahub");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final String DISPLAY_TITLE = "io.example.displayTitle";
  static final Urn DISPLAY_TITLE_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:" + DISPLAY_TITLE);
  static final String RECORD_ID = "io.example.recordId";
  static final Urn RECORD_ID_URN = UrnUtils.getUrn("urn:li:structuredProperty:" + RECORD_ID);
  static final String CLASS_ORDER = "urn:li:searchTemplate:rankingClassOrder";
  static final String CLASS_ORDER_ROUTER = "urn:li:searchTemplate:rankingClassOrderSearch";
  static final String CLASS_ORDER_NO_COPY = "urn:li:searchTemplate:rankingClassOrderNoCopy";
  static final String CLASS_ORDER_NO_COPY_ROUTER =
      "urn:li:searchTemplate:rankingClassOrderNoCopySearch";

  /** Which index and template the search box runs; each is the configured search-box default. */
  protected enum Mode {
    /** The display title is copied into the name; the class-order template infers copied values. */
    CLASS_ORDER_DEFAULT,
    /**
     * The display title is not copied anywhere; the template queries the property's own field for
     * the title tiers and the name field for the name tiers.
     */
    CLASS_ORDER_DEFAULT_NO_COPY
  }

  /** One index built from one property definition, searched through its configured default. */
  private record Stack(ElasticSearchService service, OperationContext routerContext) {}

  static final String STEWARD = "io.example.steward";
  static final String SPHYNX_QUERY = "sphynx corner";
  static final Urn STEWARD_URN = UrnUtils.getUrn("urn:li:structuredProperty:" + STEWARD);
  static final String GUID = "3f2b9c1e-7d4a-4b8e-9a61-2c5d8e0f1a7b";
  static final String HYPHENATED_ID = "PET-ADOPT-0042";

  /** More copies than the 100-row window a three- or four-word query used to get. */
  static final int EQUAL_TITLE_COPIES = 150;

  private final List<String> createdIndices = new ArrayList<>();
  private final Map<String, Dataset> corpus = new LinkedHashMap<>();
  private Stack copyStack;
  private Stack noCopyStack;

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  /** A dataset or a column, with its own name and the values the grading reads. */
  record Dataset(
      String urn,
      String entityType,
      String name,
      @Nullable String title,
      @Nullable String recordId,
      @Nullable String description,
      @Nullable String steward) {
    boolean column() {
      return SCHEMA_FIELD_ENTITY_NAME.equals(entityType);
    }
  }

  private static String datasetUrn(String name) {
    return "urn:li:dataset:(urn:li:dataPlatform:hive,petshop." + name + ",PROD)";
  }

  private void add(String name, @Nullable String title) {
    corpus.put(
        datasetUrn(name),
        new Dataset(datasetUrn(name), DATASET_ENTITY_NAME, name, title, null, null, null));
  }

  private void addDataset(
      String name,
      @Nullable String title,
      @Nullable String recordId,
      @Nullable String description) {
    corpus.put(
        datasetUrn(name),
        new Dataset(
            datasetUrn(name), DATASET_ENTITY_NAME, name, title, recordId, description, null));
  }

  /** A dataset whose key differs from its display name, e.g. a table in a schema named like it. */
  private void addKeyed(String key, String name, @Nullable String title) {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive," + key + ",PROD)";
    corpus.put(urn, new Dataset(urn, DATASET_ENTITY_NAME, name, title, null, null, null));
  }

  private void addColumn(String parent, String fieldPath, @Nullable String title) {
    String urn = "urn:li:schemaField:(" + datasetUrn(parent) + "," + fieldPath + ")";
    corpus.put(urn, new Dataset(urn, SCHEMA_FIELD_ENTITY_NAME, fieldPath, title, null, null, null));
  }

  private void buildCorpus() {
    add("kennel_booking", null);
    for (int i = 0; i < EQUAL_TITLE_COPIES; i++) {
      add(String.format("kb_listing_%03d", i), "Kennel Booking");
    }
    for (int i = 0; i < 30; i++) {
      add(String.format("kennel_booking_snapshot_%02d", i), null);
    }
    // Near misses for kennel_booking: they hold the words but are not the whole value.
    add("kennel_booking_archive", null);
    add("kennel_booking_2024", null);
    add("booking_kennel", null);
    add("kennel", null);
    add("booking", null);
    add("kba_ref", "Kennel Booking Archive");
    // The canonical example: "my cat" exact, partial matches, one equal copied title.
    add("my cat", null);
    add("my cat frank", null);
    add("awesome my cat", null);
    add("cat", null);
    add("mc_ref", "My Cat");
    add("groomed_coat_notes", null);
    add("gcn_ref", "Groomed Coat Notes");
    add("groom_coat_notes_stage", null);
    add("salon_audit_groomed", null);
    add("groomed_journal", "Groomed Journal Entries");
    for (int i = 0; i < 20; i++) {
      add(String.format("gje_ref_%02d", i), "Groomed Journal Entries " + i);
    }
    // Indexed after the datasets that only carry the title, so index order cannot put it first.
    for (int i = 0; i < 20; i++) {
      add(String.format("cr_ref_%02d", i), "Cattery Roster");
    }
    add("Cattery Roster", "Cattery Roster");
    // One query reaching every tranche, datasets and columns.
    add("sand_depth", null);
    add("sd_ref", "sand_depth");
    add("tray_logs", null);
    addColumn("tray_logs", "sand_depth", null);
    addColumn("tray_logs", "m_007", "sand_depth");
    add("sand_depth_daily", null);
    add("sdd_ref", "Sand Depth Weekly");
    addColumn("tray_logs", "sand_depth_cm", null);
    addColumn("tray_logs", "m_008", "Sand Depth Notes");
    add("sand", null);
    // An asset whose name equals the search, with a longer title of its own, against 12 tables
    // titled exactly the search in a schema named like the table family; and a control with no
    // title.
    add("ragdoll lounge", "Ragdoll Lounge Visits");
    add("siberian lounge", null);
    for (int i = 0; i < 12; i++) {
      String n = String.format("%02d", i);
      addKeyed("ragdoll_lounge.ragdoll_lounge_" + n, "ragdoll_lounge_" + n, "Ragdoll Lounge");
      addKeyed("siberian_lounge.siberian_lounge_" + n, "siberian_lounge_" + n, "Siberian Lounge");
    }
    // Longer supersets of "ragdoll lounge" in names and in titles, none of them exact.
    add("ragdoll lounge visits", null);
    for (int i = 0; i < 12; i++) {
      String n = String.format("%02d", i);
      addKeyed("ragdoll_lounge.ragdoll_lounge_visits_" + n, "ragdoll_lounge_visits_" + n, null);
      addKeyed("ragdoll_lounge.rl_" + n, "rl_" + n, "Ragdoll Lounge Visits " + n);
    }
    // No dataset is named "persian nook". Four carry the exact title, one of them with a longer
    // real name of its own; longer supersets sit in names and titles.
    for (int i = 0; i < 3; i++) {
      add(String.format("pn_ref_%02d", i), "Persian Nook");
    }
    add("pn_ref_long_name_persian_nook_history", "Persian Nook");
    add("persian nook archive", null);
    for (int i = 0; i < 12; i++) {
      String n = String.format("%02d", i);
      addKeyed("persian_nook.persian_nook_daily_" + n, "persian_nook_daily_" + n, null);
      addKeyed("persian_nook.pnw_" + n, "pnw_" + n, "Persian Nook Weekly " + n);
    }
    // "sphynx corner" has no exact match. Partial real names, partial titles, partial values of
    // another searchable property (steward), and titles whose record ID equals the search.
    for (int i = 0; i < 10; i++) {
      String n = String.format("%02d", i);
      add("sphynx_corner_daily_" + n, null);
      add("scw_" + n, "Sphynx Corner Weekly " + n);
      corpus.put(
          datasetUrn("sct_" + n),
          new Dataset(
              datasetUrn("sct_" + n),
              DATASET_ENTITY_NAME,
              "sct_" + n,
              null,
              null,
              null,
              "Sphynx Corner Team " + n));
    }
    for (int i = 0; i < 5; i++) {
      String n = String.format("%02d", i);
      addDataset("scr_" + n, "Sphynx Corner Weekly R" + n, SPHYNX_QUERY, null);
    }
    // Record identifiers, and a dataset that only mentions them in its description.
    addDataset("adoption_register", "Adoption Register", GUID, null);
    addDataset("adoption_ledger", null, HYPHENATED_ID, null);
    addDataset(
        "adoption_notes", null, null, "Cross-reference: " + GUID + " and " + HYPHENATED_ID + ".");
  }

  // ── Scenarios ───────────────────────────────────────────────────────────────

  /**
   * Typed as a user types it, without quotes: only the dataset named exactly kennel_booking is an
   * exact real-name match. Names and titles that merely hold its substrings
   * (kennel_booking_archive, booking_kennel, kennel, booking, "Kennel Booking Archive", the 30
   * snapshots) rank below it.
   */
  protected void assertUnquotedQueryExactOnlyForWholeName(Mode mode) throws Exception {
    assertTrancheOrder(mode, "kennel_booking");
  }

  /**
   * Only the title equal to the query is an exact copied match: groomed_journal, titled "Groomed
   * Journal Entries", ranks above the 20 titles "Groomed Journal Entries N".
   */
  protected void assertExactCopiedOnlyForWholeTitle(Mode mode) throws Exception {
    assertTrancheOrder(mode, "groomed journal entries");
  }

  /**
   * For "my cat": the asset named "my cat" first, the title "My Cat" next, then "my cat frank",
   * "awesome my cat" and "cat", all partial real names, in any order.
   */
  protected void assertCanonicalExampleOrder(Mode mode) throws Exception {
    assertTrancheOrder(mode, "my cat");
  }

  /**
   * For "sphynx corner", which nothing matches exactly: every partial real name
   * (sphynx_corner_daily_NN) ranks above every partial title ("Sphynx Corner Weekly NN"), including
   * five titled datasets whose record ID equals the search; no dataset matching only through
   * another searchable property (steward "Sphynx Corner Team NN") ranks above a partial title; and
   * the five record-ID datasets stay within the partial-title group.
   */
  protected void assertPartialRealNameAboveOtherPartialMatches(Mode mode) throws Exception {
    List<Dataset> served = search(mode, SPHYNX_QUERY, 0, 80).served();
    List<Integer> real = positions(served, "sphynx_corner_daily_");
    List<Integer> titles = new ArrayList<>(positions(served, "scw_"));
    List<Integer> recordIdTitles = positions(served, "scr_");
    titles.addAll(recordIdTitles);
    List<Integer> steward = positions(served, "sct_");
    String order = String.format("%s, query %s: %s", mode, SPHYNX_QUERY, groups(served));
    assertEquals(real.size(), 10, "partial real names served — " + order);
    assertEquals(titles.size(), 15, "partial titles served — " + order);
    assertTrue(
        real.stream().max(Integer::compare).get() < titles.stream().min(Integer::compare).get(),
        "(a) a partial title ranks above a partial real name — " + order);
    if (!steward.isEmpty()) {
      assertTrue(
          steward.stream().min(Integer::compare).get()
              > titles.stream().max(Integer::compare).get(),
          "(b) a steward-only match ranks above a partial title — " + order);
    }
    int titleTop = titles.stream().min(Integer::compare).get();
    int titleBottom = titles.stream().max(Integer::compare).get();
    assertTrue(
        recordIdTitles.stream().allMatch(p -> p >= titleTop && p <= titleBottom)
            && served.subList(titleTop, titleBottom + 1).stream()
                .allMatch(d -> d.name().startsWith("scw_") || d.name().startsWith("scr_")),
        "(c) the record-ID datasets leave the partial-title group — " + order);
  }

  private static List<Integer> positions(List<Dataset> served, String prefix) {
    List<Integer> out = new ArrayList<>();
    for (int i = 0; i < served.size(); i++) {
      if (served.get(i).name().startsWith(prefix)) {
        out.add(i);
      }
    }
    return out;
  }

  /** The served order as runs of one group: real name, title, title with record ID, steward. */
  private static String groups(List<Dataset> served) {
    List<String> runs = new ArrayList<>();
    String run = null;
    int count = 0;
    for (Dataset d : served) {
      String n = d.name();
      String g =
          n.startsWith("sphynx_corner_daily_")
              ? "REAL_NAME"
              : n.startsWith("scw_")
                  ? "TITLE"
                  : n.startsWith("scr_")
                      ? "TITLE_WITH_RECORD_ID"
                      : n.startsWith("sct_") ? "STEWARD_ONLY" : "OTHER(" + n + ")";
      if (g.equals(run)) {
        count++;
        continue;
      }
      if (run != null) {
        runs.add(run + " x" + count);
      }
      run = g;
      count = 1;
    }
    if (run != null) {
      runs.add(run + " x" + count);
    }
    return String.join(" > ", runs)
        + (served.stream().anyMatch(d -> d.name().startsWith("sct_"))
            ? ""
            : " (no steward-only row served)");
  }

  /** A real name equal to its own copied title is an exact real-name match. */
  protected void assertRealNameEqualToOwnTitleIsExactRealName(Mode mode) throws Exception {
    assertTrancheOrder(mode, "cattery roster");
  }

  /**
   * One query reaching every tranche: the dataset named sand_depth, the dataset titled sand_depth,
   * the column sand_depth, the column titled sand_depth, then the partial datasets, then the
   * partial columns. Datasets come before columns of the same match kind.
   */
  protected void assertColumnsFollowDatasetsOfTheSameMatch(Mode mode) throws Exception {
    assertTrancheOrder(mode, "sand_depth");
  }

  /**
   * A record ID typed as the query finds its asset first, as typed, in upper case, and for a
   * hyphenated ID. A dataset that only mentions the ID in its description ranks below it.
   */
  protected void assertRecordIdFindsItsAssetFirst(Mode mode) throws Exception {
    for (String[] c :
        new String[][] {
          {GUID, "adoption_register"},
          {GUID.toUpperCase(), "adoption_register"},
          {HYPHENATED_ID, "adoption_ledger"}
        }) {
      List<Dataset> actual = search(mode, c[0], 0, 10).served();
      int owner = indexOfName(actual, c[1]);
      int mention = indexOfName(actual, "adoption_notes");
      assertEquals(
          owner,
          0,
          String.format(
              "%s, query %s: %s should be #1%n  actual: %s",
              mode, c[0], c[1], describe(c[0], actual)));
      if (mention >= 0) {
        assertEquals(
            Math.min(owner, mention),
            owner,
            String.format("query %s: the description-only mention outranks %s", c[0], c[1]));
      }
    }
  }

  /**
   * Walking the result in pages of 10, past the old 100-row window: every graded result appears
   * exactly once, the tranche order holds across page boundaries, and the total does not change.
   */
  protected void assertPagingKeepsTrancheOrder(Mode mode) throws Exception {
    String query = "kennel_booking";
    List<Dataset> graded = corpus.values().stream().filter(d -> rank(query, d) != OTHER).toList();
    List<Dataset> walked = new ArrayList<>();
    java.util.Set<Integer> totals = new java.util.TreeSet<>();
    for (int from = 0; from < graded.size() + 10; from += 10) {
      Page page = search(mode, query, from, 10);
      totals.add(page.total());
      walked.addAll(page.served());
      if (page.served().isEmpty()) {
        break;
      }
    }
    java.util.Set<String> seen = new java.util.HashSet<>();
    List<String> repeated =
        walked.stream().map(Dataset::urn).filter(u -> !seen.add(u)).distinct().toList();
    assertEquals(repeated, List.of(), "results shown on more than one page");
    assertEquals(totals.size(), 1, "the total changed between pages: " + totals);
    List<String> missing =
        graded.stream().map(Dataset::urn).filter(u -> !seen.contains(u)).toList();
    assertEquals(missing, List.of(), "graded results never shown");
    assertTranchesMatch(mode, query, walked, "paged in tens");
  }

  /**
   * An asset whose name equals the search ranks first, even when its own title, and other assets'
   * titles and names, are longer matches that contain the search: "ragdoll lounge" (titled "Ragdoll
   * Lounge Visits") above 12 tables titled "Ragdoll Lounge" in the schema ragdoll_lounge.
   */
  protected void assertExactNameRanksFirstAboveLongerMatches(Mode mode) throws Exception {
    assertFirst(mode, "ragdoll lounge");
  }

  /**
   * A structured-property value equal to the search ranks above every asset whose name or title is
   * a longer match that is not exact. No dataset is named "persian nook"; the four titled exactly
   * "Persian Nook", including pn_ref_long_name_persian_nook_history, come before every dataset
   * named or titled with a longer superset (persian_nook_daily_NN, "Persian Nook Weekly NN",
   * "persian nook archive").
   */
  protected void assertExactTitleRanksAboveLongerMatches(Mode mode) throws Exception {
    String query = "persian nook";
    List<String> exactTitles =
        corpus.values().stream()
            .filter(d -> rank(query, d) == 2)
            .map(Dataset::name)
            .sorted()
            .toList();
    List<Dataset> served = search(mode, query, 0, 30).served();
    List<String> first =
        served.stream().limit(exactTitles.size()).map(Dataset::name).sorted().toList();
    assertEquals(
        first,
        exactTitles,
        String.format(
            "%s, query %s: the %d exact-title datasets should come before every longer match%n"
                + "  actual: %s",
            mode, query, exactTitles.size(), describe(query, served)));
  }

  /** Control for the case above: the same shape, but the asset has no title of its own. */
  protected void assertExactNameWithoutOwnTitleRanksFirst(Mode mode) throws Exception {
    assertFirst(mode, "siberian lounge");
  }

  /** The dataset named exactly {@code name} is the first result for that query. */
  private void assertFirst(Mode mode, String name) throws Exception {
    List<Dataset> firstPage = search(mode, name, 0, 10).served();
    assertEquals(
        firstPage.isEmpty() ? null : firstPage.get(0).name(),
        name,
        String.format(
            "%s, query %s: %s (EXACT_NAME) should be #1, at #%s%n  actual: %s",
            mode,
            name,
            name,
            indexOfName(search(mode, name, 0, 30).served(), name) + 1,
            describe(name, firstPage)));
  }

  /**
   * For "groomed", the 600 names that share only its stem (groom_NNNN, grooms_NNNN) rank below
   * every name or title that shares a substring with it.
   */
  protected void assertStemOnlyNamesRankBelowEveryMatch(Mode mode) throws Exception {
    assertTrancheOrder(mode, "groomed");
  }

  /**
   * Compares the served results, by tranche, with the expected tranche sequence. A tranche holds
   * ties in any order; a position in the wrong tranche fails, and the message lists the actual
   * order beside the expected one.
   */
  private void assertTrancheOrder(Mode mode, String query) throws Exception {
    List<Dataset> graded = corpus.values().stream().filter(d -> rank(query, d) != OTHER).toList();
    assertTranchesMatch(mode, query, search(mode, query, 0, graded.size()).served(), "first page");
  }

  /** Compares served results with the rule's tranche sequence; order inside a tranche is free. */
  private void assertTranchesMatch(Mode mode, String query, List<Dataset> actual, String how) {
    List<Dataset> expected =
        corpus.values().stream()
            .filter(d -> rank(query, d) != OTHER && rank(query, d) != SEPARATOR_ONLY)
            .sorted((a, b) -> Integer.compare(rank(query, a), rank(query, b)))
            .toList();
    // A name or title that differs from the query only in separators may rank either way.
    List<String> actualTranches =
        actual.stream()
            .filter(d -> rank(query, d) != SEPARATOR_ONLY)
            .limit(expected.size())
            .map(d -> className(query, d))
            .toList();
    List<String> expectedTranches = expected.stream().map(d -> className(query, d)).toList();
    assertEquals(
        actualTranches,
        expectedTranches,
        String.format(
            "%s, query %s (%s): results out of tranche order%n  expected: %s%n  actual:   %s",
            mode, query, how, describe(query, expected), describe(query, actual)));
  }

  private static int indexOfName(List<Dataset> served, String name) {
    for (int i = 0; i < served.size(); i++) {
      if (served.get(i).name().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  /** The first five results by name, then the whole list as runs of one class. */
  private String describe(String query, List<Dataset> datasets) {
    List<String> head = new ArrayList<>();
    for (int i = 0; i < Math.min(5, datasets.size()); i++) {
      Dataset d = datasets.get(i);
      head.add("#" + (i + 1) + " " + d.name() + " [" + className(query, d) + "]");
    }
    List<String> runs = new ArrayList<>();
    String run = null;
    int count = 0;
    for (Dataset d : datasets) {
      String c = className(query, d);
      if (c.equals(run)) {
        count++;
        continue;
      }
      if (run != null) {
        runs.add(run + " x" + count);
      }
      run = c;
      count = 1;
    }
    if (run != null) {
      runs.add(run + " x" + count);
    }
    return String.join(", ", head) + " | " + String.join(" > ", runs);
  }

  // ── Grading from the corpus definition ─────────────────────────────────────

  private static final String[] CLASSES = {
    "IDENTIFIER",
    "EXACT_NAME",
    "EXACT_COPIED",
    "EXACT_COLUMN",
    "EXACT_COLUMN_COPIED",
    "PARTIAL_NAME",
    "PARTIAL_COPIED",
    "PARTIAL_COLUMN",
    "PARTIAL_COLUMN_COPIED",
    "OTHER",
    "SEPARATOR_ONLY"
  };
  private static final int OTHER = 9;
  private static final int SEPARATOR_ONLY = 10;

  /**
   * Separator-delimited substrings: lowercased, accents folded, split on space, underscore, dot and
   * slash. A hyphen is not a separator, so PET-ADOPT-0042 and a GUID stay one substring.
   */
  static List<String> words(String text) {
    String folded =
        java.text.Normalizer.normalize(text, java.text.Normalizer.Form.NFD)
            .replaceAll("\\p{M}", "")
            .toLowerCase();
    return java.util.Arrays.stream(folded.split("[\\s_./]+")).filter(w -> !w.isEmpty()).toList();
  }

  /** The query as typed, without surrounding quotes. */
  static String typed(String query) {
    String q = query.trim();
    return q.length() > 1 && q.startsWith("\"") && q.endsWith("\"")
        ? q.substring(1, q.length() - 1)
        : q;
  }

  /** Exact: the whole value equals the query, ignoring case. */
  static boolean exact(String query, @Nullable String value) {
    return value != null && typed(query).equalsIgnoreCase(value.trim());
  }

  /** The same substrings as the query, differing only in separators: not part of the rule. */
  static boolean separatorOnly(String query, @Nullable String value) {
    return value != null && !exact(query, value) && words(typed(query)).equals(words(value));
  }

  /**
   * Partial: at least one separator-delimited substring of the search starts one of the value, or
   * the value starts with the search.
   */
  static boolean shares(String query, @Nullable String value) {
    if (value == null) {
      return false;
    }
    List<String> q = words(typed(query));
    List<String> v = words(value);
    return !q.isEmpty()
        && !v.isEmpty()
        && (q.stream().anyMatch(w -> v.stream().anyMatch(x -> x.startsWith(w)))
            || String.join(" ", v).startsWith(String.join(" ", q)));
  }

  /** The best tranche the asset reaches; its real name is graded before its copied title. */
  static int rank(String query, Dataset d) {
    if (d.recordId() != null && d.recordId().equalsIgnoreCase(typed(query))) {
      return 0;
    }
    int column = d.column() ? 2 : 0;
    if (exact(query, d.name())) {
      return 1 + column;
    }
    if (separatorOnly(query, d.name())) {
      return SEPARATOR_ONLY;
    }
    if (exact(query, d.title())) {
      return 2 + column;
    }
    if (separatorOnly(query, d.title())) {
      return SEPARATOR_ONLY;
    }
    if (shares(query, d.name())) {
      return d.column() ? 7 : 5;
    }
    if (shares(query, d.title())) {
      return d.column() ? 8 : 6;
    }
    return OTHER;
  }

  private String className(String query, Dataset d) {
    return d == null ? "UNKNOWN" : CLASSES[rank(query, d)];
  }

  // ── Search ─────────────────────────────────────────────────────────────────

  record Page(List<Dataset> served, int total) {}

  private Page search(Mode mode, String query, int from, int size) throws Exception {
    OperationContext context =
        (mode == Mode.CLASS_ORDER_DEFAULT ? copyStack : noCopyStack)
            .routerContext()
            .withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES)
            .withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true));
    SearchResult result =
        (mode == Mode.CLASS_ORDER_DEFAULT ? copyStack : noCopyStack)
            .service()
            .search(
                context,
                List.of(DATASET_ENTITY_NAME, SCHEMA_FIELD_ENTITY_NAME),
                query,
                null,
                (List<SortCriterion>) null,
                from,
                size);
    List<Dataset> out = new ArrayList<>();
    for (SearchEntity entity : result.getEntities()) {
      String urn = entity.getEntity().toString();
      out.add(
          corpus.getOrDefault(
              urn,
              new Dataset(urn, entity.getEntity().getEntityType(), urn, null, null, null, null)));
    }
    return new Page(out, result.getNumEntities());
  }

  // ── Templates ──────────────────────────────────────────────────────────────

  /** Same body as the e2e seeder's class-order template, with the title named by its URN. */
  static String classOrderBody() throws Exception {
    String title = "{{urn:li:structuredProperty:" + DISPLAY_TITLE + "}}.text";
    String recordId = "{{urn:li:structuredProperty:" + RECORD_ID + "}}";
    Map<String, Object> ds = Map.of("term", Map.of("_entityType", "dataset"));
    Map<String, Object> col = Map.of("term", Map.of("_entityType", "schemaField"));
    Map<String, Object> exactName =
        Map.of("term", Map.of("name.lowercase", Map.of("value", "{{normalizedKeyword}}")));
    Map<String, Object> phraseName =
        Map.of("match_phrase", Map.of("name.text", Map.of("query", "{{alphanumericSplit}}")));
    Map<String, Object> titlePhrase =
        Map.of("match_phrase", Map.of(title, Map.of("query", "{{alphanumericSplit}}")));
    List<Object> tiers = new ArrayList<>();
    tiers.add(
        tier(
            10000,
            "IDENTIFIER",
            // The stored ID keeps its case; compare the query as typed, ignoring case.
            List.of(
                ds,
                Map.of(
                    "term",
                    Map.of(recordId, Map.of("value", "{{raw}}", "case_insensitive", true)))),
            null));
    for (Object[] t :
        new Object[][] {
          {ds, "", new int[] {9000, 8000, 5500, 5000, 4000}},
          {col, "COLUMN_", new int[] {7000, 6000, 2500, 2000, 1000}}
        }) {
      Object type = t[0];
      String tag = (String) t[1];
      int[] b = (int[]) t[2];
      tiers.add(tier(b[0], tag + "EXACT_NAME", List.of(type, exactName), List.of(titlePhrase)));
      tiers.add(tier(b[1], tag + "EXACT_TITLE", List.of(type, exactName, titlePhrase), null));
      tiers.add(tier(b[2], tag + "PHRASE_NAME", List.of(type, phraseName), List.of(titlePhrase)));
      tiers.add(
          tier(
              b[3],
              tag + "PARTIAL_NAME",
              List.of(type, partial("name.text")),
              List.of(partial(title))));
      tiers.add(tier(b[4], tag + "PARTIAL_TITLE", List.of(type, partial(title)), null));
    }
    Map<String, Object> body =
        Map.of(
            "bool",
            Map.of(
                "minimum_should_match",
                1,
                "should",
                List.of(
                    Map.of("dis_max", Map.of("tie_breaker", 0.0, "queries", tiers)),
                    Map.of(
                        "match",
                        Map.of(
                            "name.text",
                            Map.of("query", "{{alphanumericSplit}}", "boost", 0.001))))));
    return MAPPER.writeValueAsString(body);
  }

  /**
   * The class-order body when the display title is not copied into the name: the name tiers read
   * only the name field, and the title tiers read the property's own field (keyword for exact,
   * {@code .text} for partial). Nothing has to be inferred, so there is no must_not.
   */
  static String classOrderNoCopyBody() throws Exception {
    String title = "{{urn:li:structuredProperty:" + DISPLAY_TITLE + "}}";
    String recordId = "{{urn:li:structuredProperty:" + RECORD_ID + "}}";
    Map<String, Object> ds = Map.of("term", Map.of("_entityType", "dataset"));
    Map<String, Object> col = Map.of("term", Map.of("_entityType", "schemaField"));
    Map<String, Object> exactName =
        Map.of("term", Map.of("name.lowercase", Map.of("value", "{{normalizedKeyword}}")));
    // The stored value keeps its case; compare the query as typed, ignoring case.
    Map<String, Object> exactTitle =
        Map.of("term", Map.of(title, Map.of("value", "{{raw}}", "case_insensitive", true)));
    List<Object> tiers = new ArrayList<>();
    tiers.add(
        tier(
            10000,
            "IDENTIFIER",
            List.of(
                ds,
                Map.of(
                    "term",
                    Map.of(recordId, Map.of("value", "{{raw}}", "case_insensitive", true)))),
            null));
    tiers.add(tier(9000, "EXACT_NAME", List.of(ds, exactName), null));
    tiers.add(tier(8000, "EXACT_TITLE", List.of(ds, exactTitle), null));
    tiers.add(tier(7000, "COLUMN_EXACT_NAME", List.of(col, exactName), null));
    tiers.add(tier(6000, "COLUMN_EXACT_TITLE", List.of(col, exactTitle), null));
    // dis_max keeps the best tier, so a row whose name also matches ranks as a partial name.
    tiers.add(tier(5000, "PARTIAL_NAME", List.of(ds, partial("name.text")), null));
    tiers.add(tier(4000, "PARTIAL_TITLE", List.of(ds, partial(title + ".text")), null));
    tiers.add(tier(2000, "COLUMN_PARTIAL_NAME", List.of(col, partial("name.text")), null));
    tiers.add(tier(1000, "COLUMN_PARTIAL_TITLE", List.of(col, partial(title + ".text")), null));
    Map<String, Object> body =
        Map.of(
            "bool",
            Map.of(
                "minimum_should_match",
                1,
                "should",
                List.of(
                    Map.of("dis_max", Map.of("tie_breaker", 0.0, "queries", tiers)),
                    Map.of(
                        "match",
                        Map.of(
                            "name.text",
                            Map.of("query", "{{alphanumericSplit}}", "boost", 0.001))))));
    return MAPPER.writeValueAsString(body);
  }

  private static Map<String, Object> partial(String field) {
    return Map.of(
        "bool",
        Map.of(
            "should",
            List.of(
                Map.of("match", Map.of(field, Map.of("query", "{{alphanumericSplit}}"))),
                Map.of(
                    "match_bool_prefix",
                    Map.of(field, Map.of("query", "{{alphanumericSplit}}"))))));
  }

  private static Map<String, Object> tier(
      int boost, String name, List<Object> filter, @Nullable List<Object> mustNot) {
    Map<String, Object> bool = new LinkedHashMap<>();
    bool.put("filter", filter);
    if (mustNot != null) {
      bool.put("must_not", mustNot);
    }
    return Map.of(
        "constant_score",
        Map.of("boost", boost, "_name", "class:" + name, "filter", Map.of("bool", bool)));
  }

  private static SearchTemplateProperties classOrderTemplate(String body) {
    return new SearchTemplateProperties()
        .setName("Class order")
        .setDescription("Strict match-class order in the engine score.")
        .setApiVersion(1)
        .setEditable(true)
        .setQuery(body)
        .setFunctionScore(
            "{\"functions\":[{\"weight\":1.0}],\"score_mode\":\"multiply\",\"boost_mode\":\"multiply\"}")
        .setRoutes(new com.linkedin.search.template.SearchTemplateRouteArray());
  }

  /** The shipped interactive router with ordinary queries sent to the class-order body. */
  private static SearchTemplateProperties classOrderRouter(String bodyUrn) {
    return new SearchTemplateProperties()
        .setName("Class order search")
        .setDescription(
            "The shipped interactive router; ordinary queries use the class-order body.")
        .setApiVersion(1)
        .setEditable(true)
        .setParent(UrnUtils.getUrn("urn:li:searchTemplate:searchLight"))
        .setDefaultRoute(
            new com.linkedin.search.template.SearchTemplateDefaultRoute()
                .setTemplate(UrnUtils.getUrn(bodyUrn)));
  }

  // ── Index setup ────────────────────────────────────────────────────────────

  @BeforeClass
  public void indexCorpus() throws Exception {
    buildCorpus();
    copyStack =
        buildStack("rankingrequirements", true, CLASS_ORDER, classOrderBody(), CLASS_ORDER_ROUTER);
    noCopyStack =
        buildStack(
            "rankingrequirementsnocopy",
            false,
            CLASS_ORDER_NO_COPY,
            classOrderNoCopyBody(),
            CLASS_ORDER_NO_COPY_ROUTER);
  }

  /**
   * Indexes the corpus into V3 indices under {@code prefix}, with the display title copied into the
   * name or not, and returns a search context whose search-box default is {@code routerUrn}.
   */
  private Stack buildStack(
      String prefix, boolean copyTitleToName, String bodyUrn, String body, String routerUrn)
      throws Exception {
    EntityIndexConfiguration entityIndex =
        V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.toBuilder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(
                V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.getV3().toBuilder()
                    .keywordReadEnabled(true)
                    .build())
            .build();
    var config =
        TEST_ES_SEARCH_CONFIG.toBuilder()
            .entityIndex(entityIndex)
            .search(
                TEST_ES_SEARCH_CONFIG.getSearch().toBuilder()
                    .v3(
                        new SearchV3Configuration()
                            .setTemplates(
                                templateConfiguration("urn:li:searchTemplate:searchLight")))
                    .build())
            .build();
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            config,
            null,
            new SearchTemplateLoader(0, SearchTemplateSource.STORAGE),
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    // The same stack with the class-order router as the search-box default, set in configuration.
    var routerConfig =
        config.toBuilder()
            .search(
                config.getSearch().toBuilder()
                    .v3(new SearchV3Configuration().setTemplates(templateConfiguration(routerUrn)))
                    .build())
            .build();
    SearchTemplateCoordinator routerCoordinator =
        new SearchTemplateCoordinator(
            routerConfig,
            null,
            new SearchTemplateLoader(0, SearchTemplateSource.STORAGE),
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver(prefix),
            entityIndex);
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    MappingsBuilder mappingsBuilder = createDelegatingMappingsBuilder(entityIndex);

    StructuredPropertyDefinition displayTitle =
        new StructuredPropertyDefinition()
            .setQualifiedName(DISPLAY_TITLE)
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(
                new UrnArray(
                    UrnUtils.getUrn("urn:li:entityType:datahub.dataset"),
                    UrnUtils.getUrn("urn:li:entityType:datahub.schemaField")))
            .setSearchConfiguration(
                copyTitleToName
                    ? new DataHubSearchConfig()
                        .setQueryByDefault(true)
                        .setCopyTo(new StringArray(List.of("name")))
                    : new DataHubSearchConfig().setQueryByDefault(true));
    StructuredPropertyDefinition recordIdDefinition =
        new StructuredPropertyDefinition()
            .setQualifiedName(RECORD_ID)
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    Map<Urn, List<RecordTemplate>> definitionAspects = new LinkedHashMap<>();
    for (String templateUrn : SearchTemplateBootstrap.urns()) {
      SearchTemplateProperties template = SearchTemplateBootstrap.get(templateUrn);
      if (template != null) {
        definitionAspects.put(UrnUtils.getUrn(templateUrn), List.of(template));
      }
    }
    definitionAspects.put(DISPLAY_TITLE_URN, List.of(displayTitle, new Status().setRemoved(false)));
    definitionAspects.put(
        RECORD_ID_URN, List.of(recordIdDefinition, new Status().setRemoved(false)));
    StructuredPropertyDefinition stewardDefinition =
        new StructuredPropertyDefinition()
            .setQualifiedName(STEWARD)
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    definitionAspects.put(STEWARD_URN, List.of(stewardDefinition, new Status().setRemoved(false)));
    definitionAspects.put(UrnUtils.getUrn(bodyUrn), List.of(classOrderTemplate(body)));
    definitionAspects.put(UrnUtils.getUrn(routerUrn), List.of(classOrderRouter(bodyUrn)));
    List<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        List.of(
            Pair.of(DISPLAY_TITLE_URN, displayTitle),
            Pair.of(RECORD_ID_URN, recordIdDefinition),
            Pair.of(STEWARD_URN, stewardDefinition));

    Map<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    for (Dataset d : corpus.values()) {
      Urn urn = UrnUtils.getUrn(d.urn());
      EntitySpec spec = entityRegistry.getEntitySpec(d.entityType());
      List<MCLItem> items = new ArrayList<>();
      items.add(
          event(
              entityRegistry,
              urn,
              EntityKeyUtils.convertUrnToEntityKey(urn, spec.getKeyAspectSpec())));
      if (d.column()) {
        items.add(event(entityRegistry, urn, new SchemaFieldInfo().setName(d.name())));
      } else {
        DatasetProperties properties = new DatasetProperties().setName(d.name());
        if (d.description() != null) {
          properties.setDescription(d.description());
        }
        items.add(event(entityRegistry, urn, properties));
      }
      StructuredPropertyValueAssignmentArray values = new StructuredPropertyValueAssignmentArray();
      if (d.title() != null) {
        values.add(assignment(DISPLAY_TITLE_URN, d.title()));
      }
      if (d.recordId() != null) {
        values.add(assignment(RECORD_ID_URN, d.recordId()));
      }
      if (d.steward() != null) {
        values.add(assignment(STEWARD_URN, d.steward()));
      }
      if (!values.isEmpty()) {
        items.add(event(entityRegistry, urn, new StructuredProperties().setProperties(values)));
      }
      events.put(urn, items);
    }

    MockAspectRetriever aspectRetriever =
        new StoredTemplateRetriever(
            definitionAspects,
            Map.of(
                UrnUtils.getUrn(bodyUrn),
                classOrderTemplate(body),
                UrnUtils.getUrn(routerUrn),
                classOrderRouter(bodyUrn)));
    aspectRetriever.setEntityRegistry(entityRegistry);
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(definitionsWithActiveUsers(aspectRetriever))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(
                propertySearchRetriever(List.of(DISPLAY_TITLE_URN, RECORD_ID_URN, STEWARD_URN)))
            .build();
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            () -> retrieverContext,
            SearchContext.builder()
                .indexConvention(indexConvention)
                .searchClusterAccess(SearchClusterAccess.fixed(getSearchClient()))
                .searchTemplateRuntime(coordinator)
                .searchableFieldTypes(
                    ESUtils.buildSearchableFieldTypes(entityRegistry, mappingsBuilder))
                .searchableFieldPaths(ESUtils.buildSearchableFieldPaths(entityRegistry))
                .build());
    OperationContext routerDefaultContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            () -> retrieverContext,
            SearchContext.builder()
                .indexConvention(indexConvention)
                .searchClusterAccess(SearchClusterAccess.fixed(getSearchClient()))
                .searchTemplateRuntime(routerCoordinator)
                .searchableFieldTypes(
                    ESUtils.buildSearchableFieldTypes(entityRegistry, mappingsBuilder))
                .searchableFieldPaths(ESUtils.buildSearchableFieldPaths(entityRegistry))
                .build());
    ESIndexBuilder indexBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            config,
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            new GitVersion("0.0.0-test", "123456", Optional.empty()));
    SettingsBuilder settingsBuilder =
        createDelegatingSettingsBuilder(entityIndex, config.getIndex(), indexConvention);
    ElasticSearchService searchService =
        new ElasticSearchService(
            indexBuilder,
            TEST_SEARCH_SERVICE_CONFIG,
            config,
            mappingsBuilder,
            settingsBuilder,
            new ESSearchDAO(
                false, config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG),
            new ESBrowseDAO(
                config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG),
            new ESWriteDAO(
                config,
                getSearchClient(),
                getBulkProcessor(),
                SearchWriteAccess.fixed(getBulkProcessor())));
    Map<String, Map<String, Object>> mappings =
        mappingsBuilder.getIndexMappings(opContext, structuredProperties).stream()
            .collect(
                Collectors.toMap(
                    MappingsBuilder.IndexMapping::getIndexName,
                    MappingsBuilder.IndexMapping::getMappings));
    for (String entityType : List.of(DATASET_ENTITY_NAME, SCHEMA_FIELD_ENTITY_NAME)) {
      String indexName =
          indexConvention.getEntityIndexNameV3(
              opContext, V3IndexKeys.resolve(entityRegistry.getEntitySpec(entityType)));
      if (createdIndices.contains(indexName)) {
        continue;
      }
      indexBuilder.buildIndex(
          opContext,
          indexBuilder.buildReindexState(
              opContext,
              indexName,
              mappings.get(indexName),
              settingsBuilder.getSettings(config.getIndex(), indexName)));
      createdIndices.add(indexName);
    }
    UpdateIndicesV3Strategy.builder()
        .v3Config(entityIndex.getV3())
        .elasticSearchService(searchService)
        .searchDocumentTransformer(
            new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH))
        .timeseriesAspectService(Mockito.mock(TimeseriesAspectService.class))
        .build()
        .processBatch(opContext, events, true);
    syncAfterWrite(getBulkProcessor());
    return new Stack(searchService, routerDefaultContext);
  }

  @AfterClass(alwaysRun = true)
  public void deleteIndices() throws Exception {
    for (String index : createdIndices) {
      getSearchClient()
          .deleteIndex(
              OperationFingerprint.EMPTY, new DeleteIndexRequest(index), RequestOptions.DEFAULT);
    }
  }

  private static StructuredPropertyValueAssignment assignment(Urn property, String value) {
    return new StructuredPropertyValueAssignment()
        .setPropertyUrn(property)
        .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(value)));
  }

  private static MCLItem event(EntityRegistry entityRegistry, Urn urn, RecordTemplate aspect) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
    AspectSpec aspectSpec =
        entitySpec.getAspectSpec(PegasusUtils.getAspectNameFromSchema(aspect.schema()));
    return TestMCL.builder()
        .urn(urn)
        .changeType(ChangeType.UPSERT)
        .metadataChangeLog(
            new MetadataChangeLog()
                .setSystemMetadata(
                    new SystemMetadata().setLastObserved(TIME).setRunId("ranking-requirements")))
        .entitySpec(entitySpec)
        .aspectSpec(aspectSpec)
        .recordTemplate(aspect)
        .auditStamp(new AuditStamp().setActor(ACTOR).setTime(TIME))
        .build();
  }

  /** The shipped slots, with {@code searchBox} as the search-box (SEARCH_ENTITIES) default. */
  private static SearchTemplateConfiguration templateConfiguration(String searchBox) {
    SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
    templates.setEnabled(true);
    SearchTemplateFixtures.setShippedLimits(templates);
    templates.setDefaults(
        Map.of(
            "fulltext", slot("urn:li:searchTemplate:search"),
            "structured", slot("urn:li:searchTemplate:structured")));
    templates.setSurfaces(
        Map.of(SearchTemplateSurfaces.SEARCH_ENTITIES, Map.of("fulltext", slot(searchBox))));
    return templates;
  }

  private static Map<String, Object> slot(String urn) {
    Map<String, Object> slot = new HashMap<>();
    slot.put("default", urn);
    return slot;
  }

  /**
   * The template loader reads a stored template as a system aspect with its record, which the plain
   * mock does not carry; this serves the class-order template the way storage would.
   */
  private static final class StoredTemplateRetriever extends MockAspectRetriever {
    private final Map<Urn, SearchTemplateProperties> templates;

    private StoredTemplateRetriever(
        Map<Urn, List<RecordTemplate>> data, Map<Urn, SearchTemplateProperties> templates) {
      super(data);
      this.templates = templates;
    }

    @Nonnull
    @Override
    public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
        @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames) {
      Map<Urn, Map<String, SystemAspect>> found =
          new HashMap<>(super.getLatestSystemAspects(context, urnAspectNames));
      for (Urn urn : urnAspectNames.keySet()) {
        SearchTemplateProperties template = templates.get(urn);
        if (template != null) {
          AuditStamp stamp = new AuditStamp().setActor(ACTOR).setTime(TIME);
          found.put(
              urn,
              Map.of(
                  SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME,
                  TestSystemAspect.builder()
                      .urn(urn)
                      .recordTemplate(template)
                      .systemMetadata(new SystemMetadata().setAspectModified(stamp))
                      .auditStamp(stamp)
                      .build()));
        }
      }
      return found;
    }
  }

  /** The stored definitions, plus the empty active-users lookup that search reads. */
  private static CachingAspectRetriever definitionsWithActiveUsers(
      MockAspectRetriever definitions) {
    CachingAspectRetriever activeUsers =
        TestOperationContexts.emptyActiveUsersAspectRetriever(definitions::getEntityRegistry);
    return new CachingAspectRetriever() {
      @Nonnull
      @Override
      public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
          @Nonnull OperationFingerprint context, Set<Urn> urns, Set<String> aspectNames) {
        Map<Urn, Map<String, Aspect>> found =
            new HashMap<>(definitions.getLatestAspectObjects(context, urns, aspectNames));
        Set<Urn> missing = new HashSet<>(urns);
        missing.removeAll(found.keySet());
        if (!missing.isEmpty()) {
          found.putAll(activeUsers.getLatestAspectObjects(context, missing, aspectNames));
        }
        return found;
      }

      @Nonnull
      @Override
      public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
          @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames) {
        return definitions.getLatestSystemAspects(context, urnAspectNames);
      }

      @Nonnull
      @Override
      public Map<Urn, Boolean> entityExists(@Nonnull OperationFingerprint context, Set<Urn> urns) {
        return definitions.entityExists(context, urns);
      }

      @Nonnull
      @Override
      public EntityRegistry getEntityRegistry() {
        return definitions.getEntityRegistry();
      }
    };
  }

  private static SearchRetriever propertySearchRetriever(List<Urn> propertyUrns) {
    return new SearchRetriever() {
      @Override
      public ScrollResult scroll(
          List<String> entities,
          Filter filters,
          String scrollId,
          Integer count,
          List<SortCriterion> sortCriteria,
          SearchFlags searchFlags) {
        SearchEntityArray results = new SearchEntityArray();
        if (scrollId == null) {
          propertyUrns.forEach(urn -> results.add(new SearchEntity().setEntity(urn)));
        }
        return new ScrollResult()
            .setEntities(results)
            .setNumEntities(results.size())
            .setPageSize(results.size());
      }
    };
  }
}
