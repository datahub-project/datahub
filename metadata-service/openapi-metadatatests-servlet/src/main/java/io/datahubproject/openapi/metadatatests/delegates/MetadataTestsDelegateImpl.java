package io.datahubproject.openapi.metadatatests.delegates;

import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.TestFetcher;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestMode;
import com.linkedin.test.TestStatus;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.delegates.EntityApiDelegateImpl;
import io.datahubproject.openapi.generated.MetadataTestEntityResultV1;
import io.datahubproject.openapi.generated.MetadataTestEntityResultV1Failing;
import io.datahubproject.openapi.generated.MetadataTestEntityResultV1Passing;
import io.datahubproject.openapi.generated.MetadataTestResultV1;
import io.datahubproject.openapi.generated.MetadataTestResultV1Entities;
import io.datahubproject.openapi.generated.ScrollTestEntityResponseV2;
import io.datahubproject.openapi.generated.SortOrder;
import io.datahubproject.openapi.generated.TestEntityRequestV2;
import io.datahubproject.openapi.generated.TestEntityResponseV2;
import io.datahubproject.openapi.generated.TestResultType;
import io.datahubproject.openapi.metadatatests.generated.controller.MetadataTestApiDelegate;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.springframework.http.ResponseEntity;

public class MetadataTestsDelegateImpl implements MetadataTestApiDelegate {

  private final EntityRegistry entityRegistry;
  private final EntityService entityService;
  private final EntitySearchService entitySearchService;
  private final EntityApiDelegateImpl<
          TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2>
      entityApiDelegate;
  private final AuthorizerChain authorizationChain;

  private final boolean restApiAuthorizationEnabled;

  private final QueryEngine queryEngine;
  private final ActionApplier actionApplier;

  private final PredicateEvaluator predicateEvaluator;

  public MetadataTestsDelegateImpl(
      EntityService entityService,
      EntitySearchService entitySearchService,
      EntityApiDelegateImpl<TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2>
          entityApiDelegate,
      boolean restApiAuthorizationEnabled,
      AuthorizerChain authorizationChain,
      QueryEngine queryEngine,
      ActionApplier actionApplier,
      PredicateEvaluator predicateEvaluator) {
    this.entityService = entityService;
    this.entitySearchService = entitySearchService;
    this.entityRegistry = entityService.getEntityRegistry();
    this.entityApiDelegate = entityApiDelegate;
    this.authorizationChain = authorizationChain;
    this.restApiAuthorizationEnabled = restApiAuthorizationEnabled;
    this.queryEngine = queryEngine;
    this.actionApplier = actionApplier;
    this.predicateEvaluator = predicateEvaluator;
  }

  public MetadataTestsDelegateImpl(
      EntityService entityService,
      EntitySearchService entitySearchService,
      EntityApiDelegateImpl<TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2>
          entityApiDelegate,
      boolean restApiAuthorizationEnabled,
      AuthorizerChain authorizationChain,
      QueryEngine queryEngine,
      ActionApplier actionApplier) {
    this(
        entityService,
        entitySearchService,
        entityApiDelegate,
        restApiAuthorizationEnabled,
        authorizationChain,
        queryEngine,
        actionApplier,
        PredicateEvaluator.getInstance());
  }

  @Override
  public ResponseEntity<MetadataTestEntityResultV1> evaluateEntity(
      String entityUrnStr, List<String> testUrnStrings, Boolean evaluateOnly) {
    Optional<MetadataTestEntityResultV1> result;

    try {
      Urn entityUrn = Urn.createFromString(entityUrnStr);
      Set<Urn> testUrns =
          testUrnStrings.stream()
              .map(
                  urnStr -> {
                    try {
                      return Urn.createFromString(urnStr);
                    } catch (URISyntaxException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(Collectors.toSet());

      Pair<Map<Urn, TestInfo>, TestEngine> testRuntime = getEngineForTests(testUrns);
      final Map<Urn, TestInfo> testinfoMap = testRuntime.getFirst();
      final TestEngine engine = testRuntime.getSecond();

      Map<Urn, com.linkedin.test.TestResults> testResultsMap =
          engine.evaluateTests(
              Set.of(entityUrn),
              Optional.ofNullable(evaluateOnly).orElse(true)
                  ? TestEngine.EvaluationMode.EVALUATE_ONLY
                  : TestEngine.EvaluationMode.DEFAULT);

      result =
          Optional.ofNullable(testResultsMap.get(entityUrn))
              .map(testsResult -> toTestResult(entityUrn, testinfoMap, testsResult));

    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return ResponseEntity.of(result);
  }

  @Override
  public ResponseEntity<MetadataTestResultV1> evaluateTest(
      String testUrnStr, List<String> entityUrnStrings, Boolean evaluateOnly) {
    Optional<MetadataTestResultV1> result;

    try {
      Urn testUrn = Urn.createFromString(testUrnStr);

      Pair<Map<Urn, TestInfo>, TestEngine> testRuntime = getEngineForTests(Set.of(testUrn));
      final TestEngine engine = testRuntime.getSecond();

      result =
          Optional.ofNullable(testRuntime.getFirst().get(testUrn))
              .map(
                  info -> {
                    Set<Urn> targetUrns =
                        entityUrnStrings.stream()
                            .map(
                                urnStr -> {
                                  try {
                                    return Urn.createFromString(urnStr);
                                  } catch (URISyntaxException e) {
                                    throw new RuntimeException(e);
                                  }
                                })
                            .collect(Collectors.toSet());

                    Map<Urn, com.linkedin.test.TestResults> testResultsMap =
                        engine.evaluateTests(
                            targetUrns,
                            Optional.ofNullable(evaluateOnly).orElse(true)
                                ? TestEngine.EvaluationMode.EVALUATE_ONLY
                                : TestEngine.EvaluationMode.DEFAULT);

                    List<Pair<Urn, com.linkedin.test.TestResults>> testResultPairs =
                        targetUrns.stream()
                            .map(targetUrn -> Pair.of(targetUrn, testResultsMap.get(targetUrn)))
                            .collect(Collectors.toList());

                    return toTestResult(testUrn, info, testResultPairs);
                  });
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return ResponseEntity.of(result);
  }

  private Pair<Map<Urn, TestInfo>, TestEngine> getEngineForTests(Set<Urn> testUrns)
      throws URISyntaxException {
    final Map<Urn, TestInfo> testInfoMap;

    if (testUrns.isEmpty()) {
      // normal all tests
      testInfoMap = Collections.emptyMap();
    } else {
      Map<Urn, EntityResponse> entityResponseMap =
          entityService.getEntitiesV2(
              Constants.TEST_ENTITY_NAME,
              testUrns,
              Set.of(Constants.TEST_INFO_ASPECT_NAME, Constants.STATUS_ASPECT_NAME));

      testInfoMap =
          entityResponseMap.keySet().stream()
              .flatMap(
                  testUrn -> {
                    Optional<Map.Entry<Urn, TestInfo>> optTest = Optional.empty();

                    boolean softDeleted =
                        optionalEnvelopedAspect(
                                entityResponseMap, testUrn, Constants.STATUS_ASPECT_NAME)
                            .flatMap(MetadataTestsDelegateImpl::optionalDataMap)
                            .map(Status::new)
                            .map(Status::isRemoved)
                            .orElse(false);

                    if (!softDeleted) {
                      optTest =
                          optionalEnvelopedAspect(
                                  entityResponseMap, testUrn, Constants.TEST_INFO_ASPECT_NAME)
                              .flatMap(MetadataTestsDelegateImpl::optionalDataMap)
                              .map(TestInfo::new)
                              .filter(
                                  testInfo ->
                                      TestMode.ACTIVE
                                          == Optional.ofNullable(testInfo.getStatus())
                                              .map(TestStatus::getMode)
                                              .orElse(TestMode.ACTIVE))
                              .map(testInfo -> Map.entry(testUrn, testInfo));
                    }

                    return optTest.stream();
                  })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    return Pair.of(testInfoMap, buildTestEngine(testInfoMap));
  }

  private TestEngine buildTestEngine(Map<Urn, TestInfo> testInfoMap) {
    final TestFetcher testFetcher;

    if (testInfoMap.isEmpty()) {
      testFetcher = new TestFetcher(entityService, entitySearchService);
    } else {
      testFetcher = new TestListFetcher(entityService, entitySearchService, testInfoMap);
    }

    return new TestEngine(
        entityService,
        testFetcher,
        new TestDefinitionParser(predicateEvaluator),
        queryEngine,
        predicateEvaluator,
        actionApplier,
        0,
        0);
  }

  private static MetadataTestResultV1 toTestResult(
      Urn testUrn, TestInfo testInfo, List<Pair<Urn, com.linkedin.test.TestResults>> testResults) {
    return MetadataTestResultV1.builder()
        .test(testUrn.toString())
        .testName(testInfo.hasName() ? testInfo.getName() : null)
        .entities(
            testResults.stream()
                .map(
                    urnTestResult -> {
                      Urn entityUrn = urnTestResult.getFirst();
                      com.linkedin.test.TestResults testResult = urnTestResult.getSecond();

                      MetadataTestResultV1Entities.MetadataTestResultV1EntitiesBuilder builder =
                          MetadataTestResultV1Entities.builder().entity(entityUrn.toString());

                      if (testResult != null) {
                        boolean passing =
                            testResult.hasPassing() && !testResult.getPassing().isEmpty();
                        boolean failing =
                            testResult.hasFailing() && !testResult.getFailing().isEmpty();

                        if (passing || failing) {
                          builder.type(
                              passing
                                  ? TestResultType.valueOf(
                                      testResult.getPassing().get(0).getType().toString())
                                  : TestResultType.valueOf(
                                      testResult.getFailing().get(0).getType().toString()));
                        }
                      }

                      return builder.build();
                    })
                .collect(Collectors.toList()))
        .build();
  }

  private static MetadataTestEntityResultV1 toTestResult(
      Urn entityUrn,
      Map<Urn, TestInfo> testInfoMap,
      @Nullable com.linkedin.test.TestResults testResult) {
    MetadataTestEntityResultV1.MetadataTestEntityResultV1Builder builder =
        MetadataTestEntityResultV1.builder().entityUrn(entityUrn.toString());

    if (testResult != null) {
      boolean passing = testResult.hasPassing() && !testResult.getPassing().isEmpty();
      boolean failing = testResult.hasFailing() && !testResult.getFailing().isEmpty();

      if (passing) {
        builder.passing(
            testResult.getPassing().stream()
                .map(
                    passResult -> {
                      Urn testUrn = passResult.getTest();
                      return MetadataTestEntityResultV1Passing.builder()
                          .test(testUrn.toString())
                          .testName(
                              testInfoMap.containsKey(testUrn)
                                  ? testInfoMap.get(testUrn).getName()
                                  : null)
                          .type(TestResultType.valueOf(passResult.getType().toString()))
                          .build();
                    })
                .collect(Collectors.toList()));
      }

      if (failing) {
        builder.failing(
            testResult.getFailing().stream()
                .map(
                    failResult -> {
                      Urn testUrn = failResult.getTest();
                      return MetadataTestEntityResultV1Failing.builder()
                          .test(testUrn.toString())
                          .testName(
                              testInfoMap.containsKey(testUrn)
                                  ? testInfoMap.get(testUrn).getName()
                                  : null)
                          .type(TestResultType.valueOf(failResult.getType().toString()))
                          .build();
                    })
                .collect(Collectors.toList()));
      }
    }

    return builder.build();
  }

  public static class TestListFetcher extends TestFetcher {
    private final List<Test> tests;

    public TestListFetcher(
        EntityService entityService,
        EntitySearchService entitySearchService,
        Map<Urn, TestInfo> testInfo) {
      super(entityService, entitySearchService);
      this.tests =
          testInfo.entrySet().stream()
              .map(entry -> new Test(entry.getKey(), entry.getValue()))
              .collect(Collectors.toList());
    }

    @Override
    public boolean isPartial() {
      return true;
    }

    @Override
    public TestFetchResult fetch(int start, int count, String query)
        throws RemoteInvocationException, URISyntaxException {
      return new TestFetchResult(
          tests.subList(start, Math.min(tests.size(), start + count)), tests.size());
    }
  }

  @Override
  public ResponseEntity<List<TestEntityResponseV2>> create(List<TestEntityRequestV2> body) {
    return entityApiDelegate.create(body);
  }

  @Override
  public ResponseEntity<Void> delete(String urn) {
    return entityApiDelegate.delete(urn);
  }

  @Override
  public ResponseEntity<TestEntityResponseV2> get(
      String urn, Boolean systemMetadata, List<String> aspects) {
    return entityApiDelegate.get(urn, systemMetadata, aspects);
  }

  @Override
  public ResponseEntity<Void> head(String urn) {
    return entityApiDelegate.head(urn);
  }

  @Override
  public ResponseEntity<ScrollTestEntityResponseV2> scroll(
      Boolean systemMetadata,
      List<String> aspects,
      Integer count,
      String scrollId,
      List<String> sort,
      SortOrder sortOrder,
      String query) {
    return entityApiDelegate.scroll(
        systemMetadata, aspects, count, scrollId, sort, sortOrder, query);
  }

  private static Optional<EnvelopedAspect> optionalEnvelopedAspect(
      Map<Urn, EntityResponse> responseMap, Urn urn, String aspectName) {
    return Optional.ofNullable(responseMap.get(urn))
        .flatMap(
            entityResponse -> Optional.ofNullable(entityResponse.getAspects().get(aspectName)));
  }

  private static Optional<DataMap> optionalDataMap(EnvelopedAspect envelopedAspect) {
    return Optional.ofNullable(envelopedAspect).map(envAspect -> envAspect.getValue().data());
  }
}
