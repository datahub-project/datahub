package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.validation.SearchTemplateValidator;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateLadderStop;
import com.linkedin.search.template.SearchTemplateLadderStopArray;
import com.linkedin.search.template.SearchTemplateProperties;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.metadata.context.RetrieverContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.testng.annotations.Test;

public class SearchTemplateLadderWalkTest extends SearchTemplateFixture {
  @Test
  public void testSelectIsAConjunctionAndStopIsADisjunction() {
    SearchTemplateLadder select = new SearchTemplateLadder();
    select.setSelect(new StringArray("scoreSort", "pageSize", "-columnFilter"));
    assertTrue(SearchTemplateLadderPredicates.selectMatches(select, "orders", null, null, 10));
    assertFalse(
        SearchTemplateLadderPredicates.selectMatches(
            select,
            "orders",
            null,
            List.of(new SortCriterion().setField("name").setOrder(SortOrder.ASCENDING)),
            10));
    assertFalse(SearchTemplateLadderPredicates.selectMatches(select, "orders", null, null, 0));

    SearchTemplateLadder stop = new SearchTemplateLadder();
    SearchTemplateLadderStop minHits = new SearchTemplateLadderStop();
    minHits.setMinHits(1);
    SearchTemplateLadderStop shape = new SearchTemplateLadderStop();
    shape.setEmptyWhen("spacelessIdentifier");
    stop.setStop(new SearchTemplateLadderStopArray(minHits, shape));
    assertTrue(SearchTemplateLadderPredicates.stopMatches(stop, 0, "a.b.c.d", null, null, null));
    assertFalse(SearchTemplateLadderPredicates.stopMatches(stop, 0, "orders", null, null, null));
    assertTrue(SearchTemplateLadderPredicates.stopMatches(stop, 2, "orders", null, null, null));
  }

  @Test
  public void testPredicateEdges() {
    assertNull(SearchTemplateLadderPredicates.base(null));
    assertNull(SearchTemplateLadderPredicates.base(""));
    assertEquals(SearchTemplateLadderPredicates.base("pageSize"), "pageSize");
    assertFalse(SearchTemplateLadderPredicates.shape("-unknown", "orders", null, null, 10));

    SearchTemplateLadder pageSize = new SearchTemplateLadder();
    pageSize.setSelect(new StringArray("pageSize"));
    assertTrue(SearchTemplateLadderPredicates.selectMatches(pageSize, "orders", null, null, null));

    assertFalse(matches("singleTerm", " "));
    assertTrue(matches("dotted", "a.b.c"));
    assertFalse(matches("dotted", "orders"));
    assertTrue(matches("identity", "urn:li:dataset:orders"));
    assertFalse(matches("identity", "orders"));

    assertThrows(
        SearchTemplateException.class,
        () -> SearchTemplatePolicyChecks.checkLadder(emptyWhen("-unknown"), null));
  }

  @Test
  public void testEmptyWhenUsesTheRequest() {
    Filter column =
        new Filter().setCriteria(new CriterionArray(new Criterion().setField("fieldPaths")));
    assertFalse(
        SearchTemplateLadderPredicates.stopMatches(
            emptyWhen("-columnFilter"), 0, "orders", column, null, 10));
    assertTrue(
        SearchTemplateLadderPredicates.stopMatches(
            emptyWhen("-columnFilter"), 0, "orders", null, null, 10));
    assertFalse(
        SearchTemplateLadderPredicates.stopMatches(
            emptyWhen("scoreSort"),
            0,
            "orders",
            null,
            List.of(new SortCriterion().setField("name").setOrder(SortOrder.ASCENDING)),
            10));
    assertFalse(
        SearchTemplateLadderPredicates.stopMatches(
            emptyWhen("pageSize"), 0, "orders", null, null, 0));
    assertTrue(
        SearchTemplateLadderPredicates.stopMatches(
            emptyWhen("pageSize"), 0, "orders", null, null, 10));
  }

  @Test
  public void testDashInvertsAPredicate() {
    assertTrue(matches("-singleTerm", "customer orders"));
    assertFalse(matches("-singleTerm", "orders"));
    assertTrue(matches("-longQuery", "orders"));
    assertTrue(matches("-longQuery", "customer orders"));
    assertFalse(matches("-longQuery", "one two three four"));
    assertTrue(matches("-columnFilter", "orders"));
    assertThrows(SearchTemplateException.class, () -> accept("-unknown"));
    assertThrows(SearchTemplateException.class, () -> accept("--longQuery"));
    assertThrows(SearchTemplateException.class, () -> accept("-"));
    assertThrows(SearchTemplateException.class, () -> accept("notColumnFilter"));
    accept("-longQuery");
  }

  @Test
  public void testLongQuerySkipsTheRungWithoutRunningIt() {
    SearchTemplateProperties light = body("Light", "{ \"match_all\": {} }", null);
    SearchTemplateLadder ladder = stopLadder(1, "urn:li:searchTemplate:fulltext");
    ladder.setSelect(new StringArray("-longQuery"));
    light.setLadder(ladder);
    store("urn:li:searchTemplate:light", light);
    store("urn:li:searchTemplate:fulltext", body("Fulltext", "{ \"match_all\": {} }", null));

    Set<String> called = ConcurrentHashMap.newKeySet();
    SearchTemplateWalk.Served<SearchResult> skipped =
        walk(
            "urn:li:searchTemplate:light",
            "one two three four",
            null,
            10,
            urn -> {
              called.add(urn);
              return page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)");
            });
    assertEquals(skipped.urn(), "urn:li:searchTemplate:fulltext");
    assertEquals(called, Set.of("urn:li:searchTemplate:fulltext"));

    called.clear();
    SearchTemplateWalk.Served<SearchResult> kept =
        walk(
            "urn:li:searchTemplate:light",
            "orders",
            null,
            10,
            urn -> {
              called.add(urn);
              return page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)");
            });
    assertEquals(kept.urn(), "urn:li:searchTemplate:light");
    assertEquals(called, Set.of("urn:li:searchTemplate:light"));
  }

  @Test
  public void testLastRungEmptyResultIsKeptAndExplicitUrnFollowsNext() {
    SearchTemplateProperties entry = body("Entry", "{ \"match_all\": {} }", null);
    entry.setLadder(stopLadder(1, "urn:li:searchTemplate:broad"));
    store("urn:li:searchTemplate:entry", entry);
    store("urn:li:searchTemplate:broad", body("Broad", "{ \"match_all\": {} }", null));

    Set<String> called = ConcurrentHashMap.newKeySet();
    SearchTemplateWalk.Served<SearchResult> served =
        walk(
            "urn:li:searchTemplate:entry",
            "orders",
            null,
            10,
            urn -> {
              called.add(urn);
              return page(
                  urn.endsWith("broad") ? 4 : 0,
                  "urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)");
            });
    assertEquals(served.urn(), "urn:li:searchTemplate:broad");
    assertEquals(called, Set.of("urn:li:searchTemplate:entry", "urn:li:searchTemplate:broad"));

    SearchTemplateProperties last = body("Last", "{ \"match_all\": {} }", null);
    last.setLadder(stopLadder(1, null));
    store("urn:li:searchTemplate:last", last);
    SearchTemplateWalk.Served<SearchResult> empty =
        walk("urn:li:searchTemplate:last", "orders", null, 10, urn -> page(0));
    assertEquals(empty.page().getNumEntities(), 0);
    assertEquals(empty.urn(), "urn:li:searchTemplate:last");

    assertThrows(
        RuntimeException.class,
        () ->
            walk(
                "urn:li:searchTemplate:last",
                "orders",
                null,
                10,
                urn -> {
                  throw new IllegalStateException("shard");
                }));
  }

  @Test
  public void testParallelAnyCancelsAndAllEmptyPrefersTheFirstListedStop() throws Exception {
    String slow = "urn:li:searchTemplate:slow";
    String fast = "urn:li:searchTemplate:fast";
    SearchTemplateProperties slowBody = body("Slow", "{ \"match_all\": {} }", null);
    slowBody.setLadder(stopLadder(1, null));
    SearchTemplateProperties fastBody = body("Fast", "{ \"match_all\": {} }", null);
    fastBody.setLadder(stopLadder(1, null));
    store(slow, slowBody);
    store(fast, fastBody);
    SearchTemplateProperties any = body("Any", "{ \"match_all\": {} }", null);
    SearchTemplateLadder anyLadder = new SearchTemplateLadder();
    anyLadder.setParallel(new UrnArray(UrnUtils.getUrn(slow), UrnUtils.getUrn(fast)));
    anyLadder.setJoin("any");
    any.setLadder(anyLadder);
    store("urn:li:searchTemplate:any", any);

    CountDownLatch slowEntered = new CountDownLatch(1);
    AtomicBoolean slowFinished = new AtomicBoolean();
    long started = System.nanoTime();
    // The common pool can have one worker, which runs these branches one after another.
    ExecutorService executor = Executors.newFixedThreadPool(2);
    SearchTemplateWalk.Served<SearchResult> served;
    try {
      served =
          walk(
              executor,
              "urn:li:searchTemplate:any",
              "orders",
              null,
              10,
              urn -> {
                if (urn.equals(slow)) {
                  slowEntered.countDown();
                  try {
                    Thread.sleep(30_000);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return page(0);
                  }
                  slowFinished.set(true);
                  return page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,slow,PROD)");
                }
                try {
                  assertTrue(slowEntered.await(2, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return page(0);
                }
                return page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,fast,PROD)");
              });
      assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5));
      assertEquals(served.urn(), fast);
      assertFalse(slowFinished.get());

      String first = "urn:li:searchTemplate:firstListed";
      String second = "urn:li:searchTemplate:secondListed";
      SearchTemplateProperties firstBody = body("First", "{ \"match_all\": {} }", null);
      firstBody.setLadder(stopLadder(1, null));
      SearchTemplateProperties secondBody = body("Second", "{ \"match_all\": {} }", null);
      secondBody.setLadder(stopLadder(1, null));
      store(first, firstBody);
      store(second, secondBody);
      SearchTemplateProperties allEmpty = body("All empty", "{ \"match_all\": {} }", null);
      SearchTemplateLadder allLadder = new SearchTemplateLadder();
      allLadder.setParallel(new UrnArray(UrnUtils.getUrn(first), UrnUtils.getUrn(second)));
      allLadder.setJoin("allEmpty");
      allEmpty.setLadder(allLadder);
      store("urn:li:searchTemplate:allEmpty", allEmpty);
      SearchTemplateWalk.Served<SearchResult> waited =
          walk(
              executor,
              "urn:li:searchTemplate:allEmpty",
              "orders",
              null,
              10,
              urn -> {
                if (urn.equals(first)) {
                  try {
                    Thread.sleep(150);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return page(0);
                  }
                }
                String name = urn.equals(first) ? "first" : "second";
                return page(1, "urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)");
              });
      assertEquals(waited.urn(), first);

      String emptyStop = "urn:li:searchTemplate:emptyStop";
      SearchTemplateProperties emptyStopBody = body("Empty stop", "{ \"match_all\": {} }", null);
      emptyStopBody.setLadder(emptyWhen("spacelessIdentifier"));
      store(emptyStop, emptyStopBody);
      SearchTemplateProperties hitsBody = body("Hits", "{ \"match_all\": {} }", null);
      hitsBody.setLadder(stopLadder(1, null));
      store("urn:li:searchTemplate:hits", hitsBody);
      SearchTemplateProperties mixed = body("Mixed", "{ \"match_all\": {} }", null);
      SearchTemplateLadder mixedLadder = new SearchTemplateLadder();
      mixedLadder.setParallel(
          new UrnArray(UrnUtils.getUrn(emptyStop), UrnUtils.getUrn("urn:li:searchTemplate:hits")));
      mixedLadder.setJoin("allEmpty");
      mixed.setLadder(mixedLadder);
      store("urn:li:searchTemplate:mixed", mixed);
      SearchTemplateWalk.Served<SearchResult> mixedServed =
          walk(
              executor,
              "urn:li:searchTemplate:mixed",
              "a.b.c.d",
              null,
              10,
              urn ->
                  urn.equals(emptyStop)
                      ? page(0)
                      : page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,hits,PROD)"));
      assertEquals(mixedServed.urn(), "urn:li:searchTemplate:hits");

      SearchTemplateProperties merge = body("Merge", "{ \"match_all\": {} }", null);
      SearchTemplateLadder mergeLadder = new SearchTemplateLadder();
      mergeLadder.setParallel(new UrnArray(UrnUtils.getUrn(first), UrnUtils.getUrn(second)));
      mergeLadder.setJoin("merge");
      mergeLadder.setNext(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN));
      merge.setLadder(mergeLadder);
      store("urn:li:searchTemplate:merge", merge);
      store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
      assertThrows(
          SearchTemplateException.class,
          () ->
              walk(
                  executor,
                  "urn:li:searchTemplate:merge",
                  "orders",
                  null,
                  10,
                  urn -> page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,member,PROD)")));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testParallelOverflowRunsOnCaller() {
    String first = "urn:li:searchTemplate:poolFirst";
    String second = "urn:li:searchTemplate:poolSecond";
    store(first, body("First", "{ \"match_all\": {} }", null));
    store(second, body("Second", "{ \"match_all\": {} }", null));
    SearchTemplateProperties group = body("Group", "{ \"match_all\": {} }", null);
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setParallel(new UrnArray(UrnUtils.getUrn(first), UrnUtils.getUrn(second)));
    ladder.setJoin("allEmpty");
    group.setLadder(ladder);
    store("urn:li:searchTemplate:poolGroup", group);

    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            1,
            1,
            60,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    String caller = Thread.currentThread().getName();
    Set<String> threads = ConcurrentHashMap.newKeySet();
    AtomicInteger started = new AtomicInteger();
    CountDownLatch releaseFirst = new CountDownLatch(1);
    try {
      walk(
          executor,
          "urn:li:searchTemplate:poolGroup",
          "orders",
          null,
          10,
          urn -> {
            threads.add(Thread.currentThread().getName());
            if (started.incrementAndGet() == 1) {
              try {
                assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              }
            } else {
              releaseFirst.countDown();
            }
            return page(0);
          });
    } finally {
      executor.shutdownNow();
    }
    assertTrue(threads.contains(caller));
  }

  @Test
  public void testAllEmptyMemberFailureFailsTheLastRungAndFollowsNext() {
    String empty = "urn:li:searchTemplate:emptySibling";
    String failing = "urn:li:searchTemplate:failingSibling";
    String next = "urn:li:searchTemplate:afterFailure";
    store(empty, body("Empty", "{ \"match_all\": {} }", null));
    store(failing, body("Failing", "{ \"match_all\": {} }", null));
    store(next, body("Next", "{ \"match_all\": {} }", null));

    SearchTemplateProperties last = body("Last group", "{ \"match_all\": {} }", null);
    SearchTemplateLadder lastLadder = new SearchTemplateLadder();
    lastLadder.setParallel(new UrnArray(UrnUtils.getUrn(empty), UrnUtils.getUrn(failing)));
    lastLadder.setJoin("allEmpty");
    last.setLadder(lastLadder);
    store("urn:li:searchTemplate:failureGroup", last);
    assertThrows(
        IllegalStateException.class,
        () ->
            walk(
                "urn:li:searchTemplate:failureGroup",
                "orders",
                null,
                10,
                urn -> {
                  if (urn.equals(failing)) {
                    throw new IllegalStateException("member failed");
                  }
                  return page(0);
                }));

    SearchTemplateProperties continued = body("Continued group", "{ \"match_all\": {} }", null);
    SearchTemplateLadder continuedLadder = new SearchTemplateLadder();
    continuedLadder.setParallel(new UrnArray(UrnUtils.getUrn(empty), UrnUtils.getUrn(failing)));
    continuedLadder.setJoin("allEmpty");
    continuedLadder.setNext(UrnUtils.getUrn(next));
    continued.setLadder(continuedLadder);
    store("urn:li:searchTemplate:failureThenNext", continued);
    SearchTemplateWalk.Served<SearchResult> served =
        walk(
            "urn:li:searchTemplate:failureThenNext",
            "orders",
            null,
            10,
            urn -> {
              if (urn.equals(failing)) {
                throw new IllegalStateException("member failed");
              }
              return urn.equals(next)
                  ? page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,next,PROD)")
                  : page(0);
            });
    assertEquals(served.urn(), next);

    String shard = "urn:li:searchTemplate:shardSibling";
    store(shard, body("Shard", "{ \"match_all\": {} }", null));
    SearchTemplateProperties shardGroup = body("Shard group", "{ \"match_all\": {} }", null);
    SearchTemplateLadder shardLadder = new SearchTemplateLadder();
    shardLadder.setParallel(new UrnArray(UrnUtils.getUrn(empty), UrnUtils.getUrn(shard)));
    shardLadder.setJoin("allEmpty");
    shardGroup.setLadder(shardLadder);
    store("urn:li:searchTemplate:shardGroup", shardGroup);
    assertThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateWalk.walk(
                opContext,
                loader,
                ForkJoinPool.commonPool(),
                "urn:li:searchTemplate:shardGroup",
                new SearchTemplateWalk.Request(
                    "orders", null, null, 10, SearchTemplateWalk.identity()),
                urn -> {
                  SearchResult result = page(0);
                  return SearchTemplateWalk.AttemptResult.ok(
                      result,
                      result.getNumEntities(),
                      new ArrayList<>(result.getEntities()),
                      List.of(),
                      urn.equals(shard),
                      null);
                },
                (first, entities, total) -> {
                  SearchResult copy = new SearchResult();
                  copy.setEntities(new SearchEntityArray(entities));
                  copy.setMetadata(first.getMetadata());
                  copy.setFrom(first.getFrom());
                  copy.setPageSize(first.getPageSize());
                  copy.setNumEntities(total);
                  return copy;
                }));
  }

  @Test
  public void testParallelLastRungKeepsAPageWhenStopDoesNotMatch() {
    String first = "urn:li:searchTemplate:unstoppedFirst";
    String second = "urn:li:searchTemplate:unstoppedSecond";
    String next = "urn:li:searchTemplate:unstoppedNext";
    SearchTemplateProperties firstBody = body("First", "{ \"match_all\": {} }", null);
    firstBody.setLadder(stopLadder(1, null));
    SearchTemplateProperties secondBody = body("Second", "{ \"match_all\": {} }", null);
    secondBody.setLadder(stopLadder(1, null));
    store(first, firstBody);
    store(second, secondBody);
    store(next, body("Next", "{ \"match_all\": {} }", null));

    SearchTemplateProperties last = body("Last empty", "{ \"match_all\": {} }", null);
    SearchTemplateLadder lastLadder = new SearchTemplateLadder();
    lastLadder.setParallel(new UrnArray(UrnUtils.getUrn(first), UrnUtils.getUrn(second)));
    lastLadder.setJoin("allEmpty");
    last.setLadder(lastLadder);
    store("urn:li:searchTemplate:unstoppedGroup", last);
    SearchTemplateWalk.Served<SearchResult> empty =
        walk("urn:li:searchTemplate:unstoppedGroup", "orders", null, 10, urn -> page(0));
    assertEquals(empty.urn(), first);
    assertEquals(empty.page().getNumEntities(), 0);

    SearchTemplateProperties continued = body("Follow", "{ \"match_all\": {} }", null);
    SearchTemplateLadder continuedLadder = new SearchTemplateLadder();
    continuedLadder.setParallel(new UrnArray(UrnUtils.getUrn(first), UrnUtils.getUrn(second)));
    continuedLadder.setJoin("allEmpty");
    continuedLadder.setNext(UrnUtils.getUrn(next));
    continued.setLadder(continuedLadder);
    store("urn:li:searchTemplate:unstoppedThenNext", continued);
    SearchTemplateWalk.Served<SearchResult> followed =
        walk(
            "urn:li:searchTemplate:unstoppedThenNext",
            "orders",
            null,
            10,
            urn ->
                urn.equals(next)
                    ? page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,next,PROD)")
                    : page(0));
    assertEquals(followed.urn(), next);

    SearchTemplateProperties any = body("Any last", "{ \"match_all\": {} }", null);
    SearchTemplateLadder anyLadder = new SearchTemplateLadder();
    anyLadder.setParallel(new UrnArray(UrnUtils.getUrn(first), UrnUtils.getUrn(second)));
    anyLadder.setJoin("any");
    any.setLadder(anyLadder);
    store("urn:li:searchTemplate:unstoppedAny", any);
    SearchTemplateWalk.Served<SearchResult> anyServed =
        walk("urn:li:searchTemplate:unstoppedAny", "orders", null, 10, urn -> page(0));
    assertEquals(anyServed.urn(), first);
    assertEquals(anyServed.page().getNumEntities(), 0);
  }

  @Test
  public void testParallelMemberCapabilityFailureFollowsNext() {
    String knn = "urn:li:searchTemplate:knnMember";
    String ready = "urn:li:searchTemplate:readyMember";
    String next = "urn:li:searchTemplate:afterCapability";
    SearchTemplateProperties knnBody = body("Knn", "{ \"match_all\": {} }", null);
    knnBody.setKnn("{ \"field\": \"embedding\" }");
    store(knn, knnBody);
    store(ready, body("Ready", "{ \"match_all\": {} }", null));
    store(next, body("Next", "{ \"match_all\": {} }", null));

    SearchTemplateProperties group = body("Capability group", "{ \"match_all\": {} }", null);
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setParallel(new UrnArray(UrnUtils.getUrn(knn), UrnUtils.getUrn(ready)));
    ladder.setJoin("allEmpty");
    ladder.setNext(UrnUtils.getUrn(next));
    group.setLadder(ladder);
    store("urn:li:searchTemplate:capabilityGroup", group);
    SearchTemplateWalk.Served<SearchResult> followed =
        walk(
            "urn:li:searchTemplate:capabilityGroup",
            "orders",
            null,
            10,
            urn -> page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,ready,PROD)"));
    assertEquals(followed.urn(), next);

    SearchTemplateProperties last = body("Capability last", "{ \"match_all\": {} }", null);
    SearchTemplateLadder lastLadder = new SearchTemplateLadder();
    lastLadder.setParallel(new UrnArray(UrnUtils.getUrn(knn), UrnUtils.getUrn(ready)));
    lastLadder.setJoin("allEmpty");
    last.setLadder(lastLadder);
    store("urn:li:searchTemplate:capabilityLast", last);
    assertThrows(
        SearchTemplateException.class,
        () -> walk("urn:li:searchTemplate:capabilityLast", "orders", null, 10, urn -> page(0)));

    SearchTemplateProperties any = body("Capability any", "{ \"match_all\": {} }", null);
    SearchTemplateLadder anyLadder = new SearchTemplateLadder();
    anyLadder.setParallel(new UrnArray(UrnUtils.getUrn(knn), UrnUtils.getUrn(ready)));
    anyLadder.setJoin("any");
    any.setLadder(anyLadder);
    store("urn:li:searchTemplate:capabilityAny", any);
    SearchTemplateWalk.Served<SearchResult> stopped =
        walk(
            "urn:li:searchTemplate:capabilityAny",
            "orders",
            null,
            10,
            urn -> page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,ready,PROD)"));
    assertEquals(stopped.urn(), ready);
  }

  @Test
  public void testScrollOmitsRescoreAndRerankStaysOnSearch() throws Exception {
    SearchTemplateProperties rescored = body("Fulltext", "{ \"match_all\": {} }", null);
    rescored.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} }, \"query_weight\": 1.0, \"rescore_query_weight\": 1.0 } }");
    store(SEARCH_TEMPLATE_FULLTEXT_URN, rescored);
    ElasticSearchConfiguration elastic = v3Config();
    useRuntime(
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty()));
    OperationContext searching =
        opContext
            .withSearchFlags(flags -> flags.setFulltext(true))
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    SearchRequest request =
        SearchRequestHandler.getBuilder(
                searching,
                TestEntitySpecBuilder.getSpec(),
                elastic,
                null,
                QueryFilterRewriteChain.EMPTY,
                TEST_SEARCH_SERVICE_CONFIG)
            .getSearchRequest(
                searching, "customer orders", null, null, null, null, null, 10, List.of());
    assertTrue(request.source().rescores() == null || request.source().rescores().isEmpty());
    assertTrue(
        request.source().sorts().stream().anyMatch(sort -> sort instanceof ScoreSortBuilder));
    assertTrue(
        request.source().sorts().stream()
            .anyMatch(
                sort ->
                    sort instanceof FieldSortBuilder field && "urn".equals(field.getFieldName())));

    Path source =
        Path.of(
            "src/main/java/com/linkedin/metadata/search/client/CachingEntitySearchService.java");
    if (!Files.exists(source)) {
      source = Path.of("metadata-io").resolve(source);
    }
    String text = Files.readString(source);
    int scroll = text.indexOf("private ScrollResult getRawScrollResults");
    int next = text.indexOf("private boolean enableCache", scroll);
    assertTrue(scroll > 0 && next > scroll);
    assertFalse(text.substring(scroll, next).contains("rerank"));
  }

  @Test
  public void testLadderRequiresStopAndRejectsDialectCycles() {
    SearchTemplateProperties missingStop = body("Missing", "{ \"match_all\": {} }", null);
    SearchTemplateLadder nextOnly = new SearchTemplateLadder();
    nextOnly.setNext(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN));
    missingStop.setLadder(nextOnly);
    assertThrows(
        SearchTemplateException.class, () -> SearchTemplateChecks.check(missingStop, 1024));

    SearchTemplateProperties parent = body("Parent", "{ \"match_all\": {} }", null);
    parent.setLadder(stopLadder(1, SEARCH_TEMPLATE_FULLTEXT_URN));
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    store("urn:li:searchTemplate:parentLadder", parent);
    SearchTemplateProperties child = parentOf("Child", "urn:li:searchTemplate:parentLadder");
    child.setLadder(new SearchTemplateLadder());
    store("urn:li:searchTemplate:childLadder", child);
    assertNull(loader.load(opContext, "urn:li:searchTemplate:childLadder").getLadder());
    assertNotNull(loader.load(opContext, "urn:li:searchTemplate:parentLadder").getLadder());

    SearchConfiguration search = new SearchConfiguration();
    SearchV3Configuration v3 = new SearchV3Configuration();
    v3.setTemplates(templates);
    search.setV3(v3);
    SearchTemplateValidator validator =
        new SearchTemplateValidator(loader, search)
            .setConfig(
                AspectPluginConfig.builder()
                    .className(SearchTemplateValidator.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName(SEARCH_TEMPLATE_ENTITY_NAME)
                                .aspectName("*")
                                .build()))
                    .build());
    RetrieverContext retrieverContext = retrieverContext();

    SearchTemplateProperties postgres = body("Pg", "select 1", null);
    postgres.setDialect(SearchTemplateDialect.POSTGRES);
    store("urn:li:searchTemplate:pg", postgres);
    SearchTemplateProperties mismatched = body("Es", "{ \"match_all\": {} }", null);
    SearchTemplateLadder toPg = stopLadder(1, "urn:li:searchTemplate:pg");
    mismatched.setLadder(toPg);
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:mismatched"),
                    SYSTEM,
                    ChangeType.UPSERT,
                    mismatched))
            .isEmpty());

    SearchTemplateProperties left = body("Left", "{ \"match_all\": {} }", null);
    left.setLadder(stopLadder(1, "urn:li:searchTemplate:right"));
    SearchTemplateProperties right = body("Right", "{ \"match_all\": {} }", null);
    right.setLadder(stopLadder(1, "urn:li:searchTemplate:left"));
    store("urn:li:searchTemplate:left", left);
    store("urn:li:searchTemplate:right", right);
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:left"), SYSTEM, ChangeType.UPSERT, left))
            .isEmpty());
  }

  private static boolean matches(String predicate, String query) {
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setSelect(new StringArray(predicate));
    return SearchTemplateLadderPredicates.selectMatches(ladder, query, null, null, 10);
  }

  private static void accept(String predicate) {
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setSelect(new StringArray(predicate));
    SearchTemplatePolicyChecks.checkLadder(ladder, null);
  }

  private static SearchTemplateLadder emptyWhen(String shape) {
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    SearchTemplateLadderStop stop = new SearchTemplateLadderStop();
    stop.setEmptyWhen(shape);
    ladder.setStop(new SearchTemplateLadderStopArray(stop));
    return ladder;
  }
}
