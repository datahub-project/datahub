package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.CORP_USER_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.search.template.RankingNormalization;
import com.linkedin.search.template.RankingNormalizationType;
import com.linkedin.search.template.RankingSignal;
import com.linkedin.search.template.RankingSignalArray;
import com.linkedin.search.template.RankingSignalKind;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchTemplateLoaderCacheTest {
  private static final String PARENT = "urn:li:searchTemplate:parent";
  private static final String CHILD = "urn:li:searchTemplate:child";
  private static final String OTHER = "urn:li:searchTemplate:other";
  private static final long TTL_MILLIS = 60_000L;

  private final AtomicLong clock = new AtomicLong(1_000L);
  private final Map<String, SearchTemplateDocuments.Document> indexed = new HashMap<>();
  private final Map<String, SearchTemplateDocuments.Document> primary = new HashMap<>();
  private final AtomicInteger documentReads = new AtomicInteger();
  private final AtomicInteger primaryReads = new AtomicInteger();
  private boolean failRead;
  private OperationContext opContext;
  private SearchTemplateLoader loader;

  @BeforeMethod
  public void setup() {
    clock.set(1_000L);
    indexed.clear();
    primary.clear();
    documentReads.set(0);
    primaryReads.set(0);
    failRead = false;
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);
    when(retriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry());
    when(retriever.getLatestAspectObject(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              primaryReads.incrementAndGet();
              Urn urn = invocation.getArgument(1);
              String aspect = invocation.getArgument(2);
              if (!SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME.equals(aspect)) {
                return null;
              }
              SearchTemplateDocuments.Document document = primary.get(urn.toString());
              if (document == null || document.properties() == null) {
                return null;
              }
              return new Aspect(document.properties().data().copy());
            });
    when(retriever.getLatestSystemAspect(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Urn urn = invocation.getArgument(1);
              String aspect = invocation.getArgument(2);
              if (!SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME.equals(aspect)) {
                return null;
              }
              SearchTemplateDocuments.Document document = primary.get(urn.toString());
              if (document == null || document.properties() == null) {
                return null;
              }
              return SearchTemplateFixtures.systemAspect(
                  urn, document.properties(), document.modifiedAt());
            });
    when(retriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              Set<String> aspects = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                Map<String, Aspect> byAspect = new HashMap<>();
                if (aspects.contains(CORP_USER_KEY_ASPECT_NAME)
                    && "corpuser".equals(urn.getEntityType())) {
                  byAspect.put(
                      CORP_USER_KEY_ASPECT_NAME,
                      new Aspect(
                          new CorpUserInfo()
                              .setActive(true)
                              .setEmail(urn.getId())
                              .setDisplayName(urn.getId())
                              .data()));
                }
                result.put(urn, byAspect);
              }
              return result;
            });
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            RetrieverContext.builder()
                .aspectRetriever(retriever)
                .cachingAspectRetriever(retriever)
                .graphRetriever(GraphRetriever.EMPTY)
                .build());
    loader = new SearchTemplateLoader(60, clock::get);
    loader.installDocuments(
        (context, urns) -> {
          documentReads.incrementAndGet();
          if (failRead) {
            throw new IllegalStateException("search unavailable");
          }
          Map<String, SearchTemplateDocuments.Document> found = new LinkedHashMap<>();
          for (String urn : urns) {
            if (!indexed.containsKey(urn)) {
              continue;
            }
            found.put(urn, copyIndexed(indexed.get(urn)));
          }
          return found;
        });
  }

  @Test
  public void testUnchangedStampsExtendTheTtlWithoutMergingAgain() {
    indexed.put(CHILD, body(CHILD, "{ \"match_all\": {} }", 1L));
    MergedSearchTemplate first = loader.load(opContext, CHILD);
    int reads = documentReads.get();
    clock.addAndGet(TTL_MILLIS + 1);
    MergedSearchTemplate refreshed = loader.load(opContext, CHILD);
    assertEquals(refreshed.getQuery(), first.getQuery());
    assertSame(refreshed, first);
    assertEquals(documentReads.get(), reads + 1);
    int afterRefresh = documentReads.get();
    assertSame(loader.load(opContext, CHILD), refreshed);
    assertEquals(documentReads.get(), afterRefresh);
  }

  @Test
  public void testSameStampWithADifferentBodyRemerges() {
    indexed.put(CHILD, body(CHILD, "{ \"match_all\": {} }", 1L));
    loader.load(opContext, CHILD);
    indexed.put(CHILD, body(CHILD, "{ \"term\": { \"name\": \"after\" } }", 1L));
    clock.addAndGet(TTL_MILLIS + 1);
    MergedSearchTemplate refreshed = loader.load(opContext, CHILD);
    assertTrue(refreshed.getQuery().contains("after"), refreshed.getQuery());
  }

  @Test
  public void testMissingLastModifiedDoesNotReuseThePreviousBody() {
    indexed.put(CHILD, unstamped(CHILD, "{ \"match_all\": {} }"));
    loader.load(opContext, CHILD);
    indexed.put(CHILD, unstamped(CHILD, "{ \"term\": { \"name\": \"after\" } }"));
    clock.addAndGet(TTL_MILLIS + 1);
    MergedSearchTemplate refreshed = loader.load(opContext, CHILD);
    assertTrue(refreshed.getQuery().contains("after"), refreshed.getQuery());
  }

  @Test
  public void testParentChangeRemergesTheChildAndKeepsAnUnrelatedTemplate() {
    indexed.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"before\" } }", 1L));
    indexed.put(CHILD, child(CHILD, PARENT, 1L));
    indexed.put(OTHER, body(OTHER, "{ \"match_all\": {} }", 1L));
    loader.load(opContext, PARENT);
    loader.load(opContext, CHILD);
    MergedSearchTemplate other = loader.load(opContext, OTHER);
    primaryReads.set(0);

    indexed.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"after\" } }", 2L));
    clock.addAndGet(TTL_MILLIS + 1);
    MergedSearchTemplate remerged = loader.load(opContext, CHILD);
    assertTrue(remerged.getQuery().contains("after"), remerged.getQuery());
    assertEquals(primaryReads.get(), 0);
    assertSame(loader.load(opContext, OTHER), other);
  }

  @Test
  public void testUnreadParentChangeRemergesTheChild() {
    primary.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"before\" } }", 1L));
    indexed.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"before\" } }", 1L));
    indexed.put(CHILD, child(CHILD, PARENT, 1L));
    MergedSearchTemplate first = loader.load(opContext, CHILD);
    assertTrue(first.getQuery().contains("before"), first.getQuery());
    assertFalse(first.getQuery().contains("after"), first.getQuery());

    indexed.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"after\" } }", 2L));
    clock.addAndGet(TTL_MILLIS + 1);
    MergedSearchTemplate remerged = loader.load(opContext, CHILD);
    assertTrue(remerged.getQuery().contains("after"), remerged.getQuery());
  }

  @Test
  public void testUnchangedUnreadParentKeepsTheMergedChild() {
    primary.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"before\" } }", 1L));
    indexed.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"before\" } }", 1L));
    indexed.put(CHILD, child(CHILD, PARENT, 1L));
    MergedSearchTemplate first = loader.load(opContext, CHILD);
    clock.addAndGet(TTL_MILLIS + 1);
    assertSame(loader.load(opContext, CHILD), first);
  }

  @Test
  public void testAMissingDocumentDropsTheTemplate() {
    indexed.put(CHILD, body(CHILD, "{ \"match_all\": {} }", 1L));
    indexed.put(OTHER, body(OTHER, "{ \"match_all\": {} }", 1L));
    loader.load(opContext, CHILD);
    loader.load(opContext, OTHER);
    indexed.remove(OTHER);
    clock.addAndGet(TTL_MILLIS + 1);
    loader.load(opContext, CHILD);
    expectThrows(SearchTemplateException.class, () -> loader.load(opContext, OTHER));
  }

  @Test
  public void testAChildWithAMissingParentIsOmitted() {
    indexed.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"kept\" } }", 1L));
    indexed.put(CHILD, child(CHILD, PARENT, 1L));
    indexed.put(OTHER, body(OTHER, "{ \"match_all\": {} }", 1L));
    loader.load(opContext, PARENT);
    assertTrue(loader.load(opContext, CHILD).getQuery().contains("kept"));
    loader.load(opContext, OTHER);
    indexed.remove(PARENT);
    clock.addAndGet(TTL_MILLIS + 1);
    Logger logger = (Logger) LoggerFactory.getLogger(SearchTemplateLoader.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      assertEquals(loader.load(opContext, OTHER).getQuery(), "{ \"match_all\": {} }");
      expectThrows(SearchTemplateException.class, () -> loader.load(opContext, CHILD));
      assertTrue(
          appender.list.stream()
              .anyMatch(
                  event ->
                      event.getLevel() == Level.WARN
                          && event.getFormattedMessage().contains(CHILD)),
          appender.list.toString());
    } finally {
      logger.detachAppender(appender);
    }
  }

  @Test
  public void testANewUrnIsFetchedWithTheWorkingSet() {
    indexed.put(CHILD, body(CHILD, "{ \"match_all\": {} }", 1L));
    indexed.put(OTHER, body(OTHER, "{ \"match_all\": {} }", 4L));
    loader.load(opContext, CHILD);
    MergedSearchTemplate other = loader.load(opContext, OTHER);
    assertEquals(other.getLastModified(), 4L);
  }

  @Test
  public void testADocumentWithoutABodyIsReadFromPrimaryStorage() {
    indexed.put(CHILD, null);
    primary.put(CHILD, body(CHILD, "{ \"match_all\": {} }", 3L));
    MergedSearchTemplate loaded = loader.load(opContext, CHILD);
    assertEquals(loaded.getQuery(), "{ \"match_all\": {} }");
    assertTrue(primaryReads.get() > 0);
  }

  @Test
  public void testACommitDuringTheReadIsVisibleOneTtlLater() {
    indexed.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"before\" } }", 1L));
    indexed.put(CHILD, child(CHILD, PARENT, 1L));
    loader.load(opContext, PARENT);
    loader.installDocuments(
        (context, urns) -> {
          documentReads.incrementAndGet();
          Map<String, SearchTemplateDocuments.Document> found = new LinkedHashMap<>();
          for (String urn : urns) {
            if (indexed.containsKey(urn)) {
              found.put(urn, copyIndexed(indexed.get(urn)));
            }
          }
          if (urns.contains(PARENT) && indexed.get(PARENT).modifiedAt() == 1L) {
            indexed.put(PARENT, body(PARENT, "{ \"term\": { \"name\": \"after\" } }", 5_000L));
            clock.set(5_000L);
          }
          return found;
        });

    MergedSearchTemplate first = loader.load(opContext, CHILD);
    assertTrue(first.getQuery().contains("before"), first.getQuery());
    assertTrue(loader.load(opContext, CHILD).getQuery().contains("before"));
    clock.set(5_000L + TTL_MILLIS + 1);
    MergedSearchTemplate next = loader.load(opContext, CHILD);
    assertTrue(next.getQuery().contains("after"), next.getQuery());
    assertEquals(next.getLastModified(), 5_000L);
  }

  @Test
  public void testARankingReferenceEditRemergesTheLeaf() {
    String target = "urn:li:searchTemplate:rescore-default";
    indexed.put(target, ranked(target, 1.0, 1L));
    primary.put(target, ranked(target, 1.0, 1L));
    indexed.put(CHILD, reference(CHILD, target, 1L));
    primary.put(CHILD, reference(CHILD, target, 1L));

    MergedSearchTemplate first = loader.load(opContext, CHILD);
    assertEquals(first.getRanking().getSignals().get(0).getBoost(), 1.0);
    assertTrue(first.getChain().contains(target));

    indexed.put(target, ranked(target, 2.0, 2L));
    primary.put(target, ranked(target, 2.0, 2L));
    clock.set(1_000L + TTL_MILLIS + 1);
    MergedSearchTemplate next = loader.load(opContext, CHILD);
    assertEquals(next.getRanking().getSignals().get(0).getBoost(), 2.0);
    assertEquals(next.getLastModified(), 2L);
  }

  @Test
  public void testAChildRankingReferenceReplacesAnInheritedPreset() {
    String library = "urn:li:searchTemplate:library";
    indexed.put(PARENT, presetNone(PARENT, 1L));
    primary.put(PARENT, presetNone(PARENT, 1L));
    indexed.put(library, ranked(library, 1.5, 1L));
    primary.put(library, ranked(library, 1.5, 1L));
    SearchTemplateDocuments.Document child = reference(CHILD, library, 1L);
    child.properties().setParent(UrnUtils.getUrn(PARENT));
    indexed.put(CHILD, child);
    primary.put(CHILD, child);

    MergedSearchTemplate loaded = loader.load(opContext, CHILD);
    assertFalse(loaded.getRanking().hasPreset());
    assertEquals(loaded.getRanking().getSignals().get(0).getBoost(), 1.5);
  }

  @Test
  public void testAnInlineChildRankingDropsAnInheritedReference() {
    String library = "urn:li:searchTemplate:library";
    indexed.put(library, ranked(library, 1.5, 1L));
    primary.put(library, ranked(library, 1.5, 1L));
    indexed.put(PARENT, reference(PARENT, library, 1L));
    primary.put(PARENT, reference(PARENT, library, 1L));
    indexed.put(CHILD, presetNone(CHILD, 1L));
    SearchTemplateDocuments.Document child = indexed.get(CHILD);
    child.properties().setParent(UrnUtils.getUrn(PARENT));
    primary.put(CHILD, child);

    MergedSearchTemplate loaded = loader.load(opContext, CHILD);
    assertEquals(loaded.getRanking().getPreset(), "none");
    assertFalse(loaded.getRanking().hasSignals());
    assertFalse(loaded.getRanking().hasTemplate());
  }

  @Test
  public void testAnEmptySignalListDropsSignalsAndKeepsTheRanking() {
    SearchTemplateDocuments.Document parent = presetNone(PARENT, 1L);
    parent.properties().getRanking().setWindow(40);
    parent.properties().getRanking().setWindowSizer("fixed");
    parent.properties().getRanking().setFormula("usage");
    parent.properties().setCapabilities(new StringArray("rankingFormula"));
    indexed.put(PARENT, parent);
    primary.put(PARENT, parent);
    SearchTemplateProperties childProperties = properties(CHILD, null);
    childProperties.setParent(UrnUtils.getUrn(PARENT));
    childProperties.setRanking(new SearchTemplateRanking().setSignals(new RankingSignalArray()));
    SearchTemplateDocuments.Document child =
        new SearchTemplateDocuments.Document(childProperties, 1L);
    indexed.put(CHILD, child);
    primary.put(CHILD, child);

    MergedSearchTemplate loaded = loader.load(opContext, CHILD);
    assertEquals(loaded.getRanking().getPreset(), "none");
    assertEquals(loaded.getRanking().getWindow(), Integer.valueOf(40));
    assertEquals(loaded.getRanking().getWindowSizer(), "fixed");
    assertEquals(loaded.getRanking().getFormula(), "usage");
    assertFalse(loaded.getRanking().hasSignals());
  }

  @Test
  public void testAnEmptySignalListKeepsInheritedReferencePatches() {
    String library = "urn:li:searchTemplate:library";
    SearchTemplateDocuments.Document libraryDocument = ranked(library, 1.5, 1L);
    RankingSignal kept = new RankingSignal();
    kept.setName("viewCount");
    kept.setKind(RankingSignalKind.FIELD_NUMBER);
    kept.setField("viewCount");
    kept.setNormalization(new RankingNormalization().setType(RankingNormalizationType.NONE));
    kept.setBoost(2.0);
    libraryDocument.properties().getRanking().getSignals().add(kept);
    indexed.put(library, libraryDocument);
    primary.put(library, libraryDocument);
    SearchTemplateDocuments.Document parent = reference(PARENT, library, 1L);
    parent.properties().getRanking().setRemove(new StringArray("queryCountPercentileLast30Days"));
    indexed.put(PARENT, parent);
    primary.put(PARENT, parent);
    SearchTemplateProperties childProperties = properties(CHILD, null);
    childProperties.setParent(UrnUtils.getUrn(PARENT));
    childProperties.setRanking(new SearchTemplateRanking().setSignals(new RankingSignalArray()));
    SearchTemplateDocuments.Document child =
        new SearchTemplateDocuments.Document(childProperties, 1L);
    indexed.put(CHILD, child);
    primary.put(CHILD, child);

    MergedSearchTemplate loaded = loader.load(opContext, CHILD);
    assertEquals(loaded.getRanking().getSignals().size(), 1);
    assertEquals(loaded.getRanking().getSignals().get(0).getName(), "viewCount");
  }

  @Test
  public void testAnEmptyFormulaDropsTheFormulaAndKeepsSignals() {
    SearchTemplateDocuments.Document parent = ranked(PARENT, 1.5, 1L);
    parent.properties().getRanking().setFormula("usage");
    parent.properties().getCapabilities().add("rankingFormula");
    indexed.put(PARENT, parent);
    primary.put(PARENT, parent);
    SearchTemplateProperties childProperties = properties(CHILD, "{ \"match_all\": {} }");
    childProperties.setParent(UrnUtils.getUrn(PARENT));
    childProperties.setRanking(new SearchTemplateRanking().setFormula(""));
    SearchTemplateDocuments.Document child =
        new SearchTemplateDocuments.Document(childProperties, 1L);
    indexed.put(CHILD, child);
    primary.put(CHILD, child);

    MergedSearchTemplate loaded = loader.load(opContext, CHILD);
    assertEquals(loaded.getRanking().getSignals().get(0).getBoost(), 1.5);
    assertFalse(loaded.getRanking().hasFormula());
  }

  @Test
  public void testAnEmptyTemplateDropsReferencePatches() {
    String library = "urn:li:searchTemplate:library";
    indexed.put(library, ranked(library, 1.5, 1L));
    primary.put(library, ranked(library, 1.5, 1L));
    SearchTemplateDocuments.Document parent = reference(PARENT, library, 1L);
    parent.properties().getRanking().setRemove(new StringArray("queryCountPercentileLast30Days"));
    indexed.put(PARENT, parent);
    primary.put(PARENT, parent);
    SearchTemplateProperties childProperties = properties(CHILD, null);
    childProperties.setParent(UrnUtils.getUrn(PARENT));
    SearchTemplateRanking ranking = new SearchTemplateRanking();
    ranking.setTemplate("");
    ranking.setWindow(10);
    ranking.setWindowSizer("fixed");
    childProperties.setRanking(ranking);
    SearchTemplateDocuments.Document child =
        new SearchTemplateDocuments.Document(childProperties, 1L);
    indexed.put(CHILD, child);
    primary.put(CHILD, child);

    MergedSearchTemplate loaded = loader.load(opContext, CHILD);
    assertEquals(loaded.getRanking().getWindow(), Integer.valueOf(10));
    assertEquals(loaded.getRanking().getWindowSizer(), "fixed");
    assertFalse(loaded.getRanking().hasTemplate());
    assertFalse(loaded.getRanking().hasRemove());
    assertFalse(loaded.getRanking().hasSignals());
  }

  @Test
  public void testSignalsLeaveAnInheritedPresetUnset() {
    indexed.put(PARENT, presetNone(PARENT, 1L));
    primary.put(PARENT, presetNone(PARENT, 1L));
    SearchTemplateDocuments.Document child = ranked(CHILD, 2.5, 1L);
    child.properties().setParent(UrnUtils.getUrn(PARENT));
    indexed.put(CHILD, child);
    primary.put(CHILD, child);

    MergedSearchTemplate loaded = loader.load(opContext, CHILD);
    assertFalse(loaded.getRanking().hasPreset());
    assertEquals(loaded.getRanking().getSignals().get(0).getBoost(), 2.5);
  }

  @Test
  public void testARankingReferenceAncestorEditRemergesTheLeaf() {
    String library = "urn:li:searchTemplate:library";
    indexed.put(PARENT, ranked(PARENT, 1.0, 1L));
    primary.put(PARENT, ranked(PARENT, 1.0, 1L));
    indexed.put(library, child(library, PARENT, 1L));
    primary.put(library, child(library, PARENT, 1L));
    indexed.put(CHILD, reference(CHILD, library, 1L));
    primary.put(CHILD, reference(CHILD, library, 1L));

    MergedSearchTemplate first = loader.load(opContext, CHILD);
    assertEquals(first.getRanking().getSignals().get(0).getBoost(), 1.0);
    assertTrue(first.getChain().contains(library));
    assertTrue(first.getChain().contains(PARENT));
    assertEquals(first.getLastModified(), 1L);

    indexed.put(PARENT, ranked(PARENT, 4.0, 4L));
    primary.put(PARENT, ranked(PARENT, 4.0, 4L));
    clock.set(1_000L + TTL_MILLIS + 1);
    MergedSearchTemplate next = loader.load(opContext, CHILD);
    assertEquals(next.getRanking().getSignals().get(0).getBoost(), 4.0);
    assertEquals(next.getLastModified(), 4L);
  }

  @Test
  public void testAFailedReloadKeepsThePreviousSnapshot() {
    indexed.put(CHILD, body(CHILD, "{ \"match_all\": {} }", 1L));
    MergedSearchTemplate first = loader.load(opContext, CHILD);
    failRead = true;
    clock.addAndGet(TTL_MILLIS + 1);
    Logger logger = (Logger) LoggerFactory.getLogger(SearchTemplateLoader.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      assertSame(loader.load(opContext, CHILD), first);
      assertTrue(
          appender.list.stream().anyMatch(event -> event.getLevel() == Level.WARN),
          appender.list.toString());
    } finally {
      logger.detachAppender(appender);
    }
    expectThrows(IllegalStateException.class, () -> loader.load(opContext, OTHER));
  }

  private static SearchTemplateDocuments.Document copyIndexed(
      SearchTemplateDocuments.Document document) {
    if (document == null || document.properties() == null) {
      return new SearchTemplateDocuments.Document(
          null, document == null ? 0L : document.modifiedAt());
    }
    try {
      return new SearchTemplateDocuments.Document(
          document.properties().copy(), document.modifiedAt());
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }

  private static SearchTemplateDocuments.Document unstamped(String urn, String query) {
    return new SearchTemplateDocuments.Document(properties(urn, query), 0L);
  }

  private static SearchTemplateDocuments.Document body(String urn, String query, long modified) {
    return new SearchTemplateDocuments.Document(properties(urn, query), modified);
  }

  private static SearchTemplateDocuments.Document child(String urn, String parent, long modified) {
    SearchTemplateProperties properties = properties(urn, null);
    properties.setParent(UrnUtils.getUrn(parent));
    return new SearchTemplateDocuments.Document(properties, modified);
  }

  private static SearchTemplateDocuments.Document presetNone(String urn, long modified) {
    SearchTemplateProperties properties = properties(urn, "{ \"match_all\": {} }");
    SearchTemplateRanking ranking = new SearchTemplateRanking();
    ranking.setPreset("none");
    properties.setRanking(ranking);
    return new SearchTemplateDocuments.Document(properties, modified);
  }

  private static SearchTemplateDocuments.Document ranked(String urn, double boost, long modified) {
    SearchTemplateProperties properties = properties(urn, null);
    properties.setCapabilities(new StringArray("rankingSignals"));
    RankingSignal signal = new RankingSignal();
    signal.setName("queryCountPercentileLast30Days");
    signal.setKind(RankingSignalKind.FIELD_NUMBER);
    signal.setField("queryCountPercentileLast30Days");
    signal.setNormalization(new RankingNormalization().setType(RankingNormalizationType.NONE));
    signal.setBoost(boost);
    SearchTemplateRanking ranking = new SearchTemplateRanking();
    ranking.setSignals(new RankingSignalArray(signal));
    properties.setRanking(ranking);
    return new SearchTemplateDocuments.Document(properties, modified);
  }

  private static SearchTemplateDocuments.Document reference(
      String urn, String target, long modified) {
    SearchTemplateProperties properties = properties(urn, "{ \"match_all\": {} }");
    properties.setCapabilities(new StringArray("rankingSignals"));
    SearchTemplateRanking ranking = new SearchTemplateRanking();
    ranking.setTemplate(target);
    properties.setRanking(ranking);
    return new SearchTemplateDocuments.Document(properties, modified);
  }

  private static SearchTemplateProperties properties(String urn, String query) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(urn.substring(urn.lastIndexOf(':') + 1));
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    if (query != null) {
      properties.setQuery(query);
    }
    try {
      return properties.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }
}
