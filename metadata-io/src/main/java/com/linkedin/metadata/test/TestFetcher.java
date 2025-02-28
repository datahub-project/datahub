package com.linkedin.metadata.test;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TestFetcher {

  private static final String LAST_UPDATED_TIME_FIELD = "lastUpdatedTimestamp";

  private final EntityService<?> _entityService;
  private final EntitySearchService _entitySearchService;

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
          (EntityResponse)
              _entityService
                  .getEntitiesV2(
                      opContext,
                      TEST_ENTITY_NAME,
                      ImmutableSet.of(testUrn),
                      ImmutableSet.of(TEST_INFO_ASPECT_NAME))
                  .getOrDefault(testUrn, null);
      if (entityResponse == null) {
        return new TestFetchResult(Collections.emptyList(), 0);
      }
      return new TestFetchResult(Collections.singletonList(extractTest(entityResponse)), 1);
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
        _entitySearchService.search(
            systemOpContext.withSearchFlags(flags -> flags.setFulltext(false)),
            List.of(TEST_ENTITY_NAME),
            query,
            null,
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
        _entityService.getEntitiesV2(
            systemOpContext,
            TEST_ENTITY_NAME,
            new HashSet<>(testUrns),
            ImmutableSet.of(TEST_INFO_ASPECT_NAME));
    return new TestFetchResult(
        testUrns.stream()
            .map(testEntities::get)
            .filter(Objects::nonNull)
            .map(this::extractTest)
            .filter(Objects::nonNull)
            .collect(Collectors.toList()),
        result.getNumEntities());
  }

  protected Test extractTest(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(TEST_INFO_ASPECT_NAME)) {
      // Right after deleting the policy, there could be a small time frame where search and local
      // db is not consistent.
      // Simply return null in that case
      return null;
    }
    return new Test(
        entityResponse.getUrn(),
        new TestInfo(aspectMap.get(TEST_INFO_ASPECT_NAME).getValue().data()));
  }

  @Value
  public static class TestFetchResult {
    List<Test> tests;
    int total;
  }

  @Value
  public static class Test {
    Urn urn;
    TestInfo testInfo;
  }
}
