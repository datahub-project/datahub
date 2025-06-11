package com.linkedin.metadata.test;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestMode;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TestFetcher {

  private static final String LAST_UPDATED_TIME_FIELD = "lastUpdatedTimestamp";
  private static final String MODE_FIELD = "mode";

  private final EntityService<?> entityService;
  private final EntitySearchService entitysearchservice;

  private static final SortCriterion SORT_CRITERION =
      new SortCriterion().setField(LAST_UPDATED_TIME_FIELD).setOrder(SortOrder.DESCENDING);

  /**
   * Returns less than all tests
   *
   * @return
   */
  public boolean isPartial() {
    return false;
  }

  public TestFetchResult fetchOne(@Nonnull OperationContext opContext, Urn testUrn) {
    try {
      EntityResponse entityResponse =
          entityService
              .getEntitiesV2(
                  opContext,
                  TEST_ENTITY_NAME,
                  ImmutableSet.of(testUrn),
                  ImmutableSet.of(TEST_INFO_ASPECT_NAME))
              .getOrDefault(testUrn, null);

      return extractTest(entityResponse)
          .map(test -> new TestFetchResult(Collections.singletonList(test), 1))
          .orElse(TestFetchResult.EMPTY);
    } catch (URISyntaxException e) {
      log.error("Failed to fetch test with urn: {}", testUrn, e);
      return new TestFetchResult(Collections.emptyList(), 0);
    }
  }

  public TestFetchResult fetch(@Nonnull OperationContext systemOpContext, int start, int count)
      throws RemoteInvocationException, URISyntaxException {
    return fetch(systemOpContext, start, count, "");
  }

  public TestFetchResult fetch(
      @Nonnull OperationContext systemOpContext, int start, int count, String query)
      throws RemoteInvocationException, URISyntaxException {
    log.debug("Batch fetching tests. start: {}, count: {}", start, count);
    // First fetch all test urns from start - start + count
    SearchResult result =
        entitysearchservice.search(
            systemOpContext.withSearchFlags(flags -> flags.setFulltext(false)),
            List.of(TEST_ENTITY_NAME),
            query,
            buildActiveFilter(),
            Collections.singletonList(SORT_CRITERION),
            start,
            count);
    List<Urn> testUrns =
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());

    if (testUrns.isEmpty()) {
      return new TestFetchResult(Collections.emptyList(), 0);
    }

    // Fetch aspects for each urn
    final Map<Urn, EntityResponse> testEntities =
        entityService.getEntitiesV2(
            systemOpContext,
            TEST_ENTITY_NAME,
            new HashSet<>(testUrns),
            ImmutableSet.of(TEST_INFO_ASPECT_NAME));

    List<Test> extractedTests =
        testUrns.stream()
            .map(testEntities::get)
            .flatMap(resp -> extractTest(resp).stream())
            .collect(Collectors.toList());

    return new TestFetchResult(extractedTests, extractedTests.size());
  }

  private Filter buildActiveFilter() {
    Filter filter = new Filter();

    CriterionArray nonExists = new CriterionArray();
    nonExists.add(CriterionUtils.buildNotExistsCriterion(MODE_FIELD));

    CriterionArray active = new CriterionArray();
    active.add(
        CriterionUtils.buildCriterion(MODE_FIELD, Condition.EQUAL, TestMode.ACTIVE.toString()));

    filter.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(
                new ConjunctiveCriterion().setAnd(nonExists),
                new ConjunctiveCriterion().setAnd(active))));

    return filter;
  }

  protected Optional<Test> extractTest(@Nullable EntityResponse entityResponse) {
    if (entityResponse != null) {
      EnvelopedAspectMap aspectMap = entityResponse.getAspects();
      // Right after deleting the policy, there could be a small time frame where search and local
      // db is not consistent.
      if (aspectMap.containsKey(TEST_INFO_ASPECT_NAME)) {
        TestInfo testInfo = new TestInfo((aspectMap.get(TEST_INFO_ASPECT_NAME).getValue().data()));
        if (testInfo.getStatus() == null || testInfo.getStatus().getMode() != TestMode.INACTIVE) {
          return Optional.of(
              new Test(
                  entityResponse.getUrn(),
                  new TestInfo(aspectMap.get(TEST_INFO_ASPECT_NAME).getValue().data())));
        }
      }
    }

    return Optional.empty();
  }

  @Value
  public static class TestFetchResult {
    public static final TestFetchResult EMPTY = new TestFetchResult(Collections.emptyList(), 0);

    List<Test> tests;
    int total;
  }

  @Value
  public static class Test {
    Urn urn;
    TestInfo testInfo;
  }
}
