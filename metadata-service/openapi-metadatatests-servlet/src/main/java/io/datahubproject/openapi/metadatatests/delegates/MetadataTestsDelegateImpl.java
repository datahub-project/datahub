package io.datahubproject.openapi.metadatatests.delegates;

import static com.linkedin.metadata.authorization.PoliciesConfig.MANAGE_TESTS_PRIVILEGE;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.TestFetcher;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestMode;
import com.linkedin.test.TestResults;
import com.linkedin.test.TestStatus;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
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
import io.datahubproject.openapi.v2.delegates.EntityApiDelegateImpl;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.http.ResponseEntity;

public class MetadataTestsDelegateImpl implements MetadataTestApiDelegate {

  private final OperationContext systemOpContext;
  private final TestsConfiguration testsConfiguration;
  private final Authorizer authorizationChain;
  private final EntityService<?> entityService;
  private final EntitySearchService entitySearchService;

  private final TimeseriesAspectService timeseriesAspectService;
  private final EntityApiDelegateImpl<
          TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2>
      entityApiDelegate;

  private final QueryEngine queryEngine;
  private final ActionApplier actionApplier;
  private final ExecutorService actionsExecutorService;

  private final PredicateEvaluator predicateEvaluator;

  public MetadataTestsDelegateImpl(
      @Nonnull OperationContext systemOpContext,
      @Nonnull TestsConfiguration testsConfiguration,
      @Nonnull Authorizer authorizationChain,
      EntityService<?> entityService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      EntityApiDelegateImpl<TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2>
          entityApiDelegate,
      QueryEngine queryEngine,
      ActionApplier actionApplier,
      PredicateEvaluator predicateEvaluator,
      @Nonnull ExecutorService actionsExecutorService) {
    this.systemOpContext = systemOpContext;
    this.testsConfiguration = testsConfiguration;
    this.authorizationChain = authorizationChain;
    this.entityService = entityService;
    this.entitySearchService = entitySearchService;
    this.timeseriesAspectService = timeseriesAspectService;
    this.entityApiDelegate = entityApiDelegate;
    this.queryEngine = queryEngine;
    this.actionApplier = actionApplier;
    this.predicateEvaluator = predicateEvaluator;
    this.actionsExecutorService = actionsExecutorService;
  }

  public MetadataTestsDelegateImpl(
      @Nonnull OperationContext systemOpContext,
      @Nonnull TestsConfiguration testsConfiguration,
      @Nonnull Authorizer authorizationChain,
      EntityService<?> entityService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      EntityApiDelegateImpl<TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2>
          entityApiDelegate,
      QueryEngine queryEngine,
      ActionApplier actionApplier,
      @Nonnull ExecutorService actionsExecutorService) {
    this(
        systemOpContext,
        testsConfiguration,
        authorizationChain,
        entityService,
        entitySearchService,
        timeseriesAspectService,
        entityApiDelegate,
        queryEngine,
        actionApplier,
        PredicateEvaluator.getInstance(),
        actionsExecutorService);
  }

  @Override
  public ResponseEntity<MetadataTestEntityResultV1> evaluateEntity(
      HttpServletRequest request,
      String entityUrnStr,
      List<String> testUrnStrings,
      Boolean evaluateOnly) {
    Optional<MetadataTestEntityResultV1> result;

    try {
      Urn entityUrn = Urn.createFromString(entityUrnStr);

      final Authentication auth = AuthenticationContext.getAuthentication();
      OperationContext opContext =
          OperationContext.asSession(
              systemOpContext,
              RequestContext.builder()
                  .buildOpenapi(
                      auth.getActor().toUrnStr(),
                      request,
                      "evaluateEntity",
                      entityUrn.getEntityType()),
              authorizationChain,
              auth,
              true);

      if (!AuthUtil.isAPIOperationsAuthorized(opContext, MANAGE_TESTS_PRIVILEGE)) {
        throw new UnauthorizedException(
            auth.getActor().toUrnStr() + " is unauthorized evaluate tests.");
      }

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

      Pair<Map<Urn, TestInfo>, TestEngine> testRuntime = getEngineForTests(opContext, testUrns);
      final Map<Urn, TestInfo> testinfoMap = testRuntime.getFirst();
      final TestEngine engine = testRuntime.getSecond();

      Map<Urn, com.linkedin.test.TestResults> testResultsMap =
          engine.evaluateTests(
              opContext,
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
      HttpServletRequest request,
      String testUrnStr,
      Boolean evaluateOnly,
      List<String> entityUrnStrings) {
    Optional<MetadataTestResultV1> result;

    try {
      Urn testUrn = Urn.createFromString(testUrnStr);

      final Authentication auth = AuthenticationContext.getAuthentication();
      OperationContext opContext =
          OperationContext.asSession(
              systemOpContext,
              RequestContext.builder()
                  .buildOpenapi(
                      auth.getActor().toUrnStr(), request, "evaluateTest", testUrn.getEntityType()),
              authorizationChain,
              auth,
              true);

      if (!AuthUtil.isAPIOperationsAuthorized(opContext, MANAGE_TESTS_PRIVILEGE)) {
        throw new UnauthorizedException(
            auth.getActor().toUrnStr() + " is unauthorized evaluate tests.");
      }

      Pair<Map<Urn, TestInfo>, TestEngine> testRuntime =
          getEngineForTests(opContext, Set.of(testUrn));
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
                    Map<Urn, com.linkedin.test.TestResults> testResultsMap;
                    List<Pair<Urn, com.linkedin.test.TestResults>> testResultPairs;

                    if (targetUrns.isEmpty()) {
                      log.info(
                          "No target urns provided, will evaluate for all entities. Test: {}",
                          testUrn);
                      testResultsMap =
                          engine.evaluateSingleTest(
                              opContext,
                              testUrn,
                              Optional.ofNullable(evaluateOnly).orElse(true)
                                  ? TestEngine.EvaluationMode.EVALUATE_ONLY
                                  : TestEngine.EvaluationMode.DEFAULT);
                      testResultPairs = new ArrayList<>();
                      testResultsMap.forEach(
                          (urn, testResult) -> testResultPairs.add(Pair.of(urn, testResult)));
                    } else {
                      testResultsMap =
                          engine.evaluateTests(
                              opContext,
                              targetUrns,
                              Optional.ofNullable(evaluateOnly).orElse(true)
                                  ? TestEngine.EvaluationMode.EVALUATE_ONLY
                                  : TestEngine.EvaluationMode.DEFAULT);

                      testResultPairs =
                          targetUrns.stream()
                              .map(targetUrn -> Pair.of(targetUrn, testResultsMap.get(targetUrn)))
                              .collect(Collectors.toList());
                    }

                    return toTestResult(testUrn, info, testResultPairs);
                  });
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return ResponseEntity.of(result);
  }

  private Pair<Map<Urn, TestInfo>, TestEngine> getEngineForTests(
      @Nonnull OperationContext opContext, Set<Urn> testUrns) throws URISyntaxException {
    final Map<Urn, TestInfo> testInfoMap;

    if (testUrns.isEmpty()) {
      // normal all tests
      testInfoMap = Collections.emptyMap();
    } else {
      Map<Urn, EntityResponse> entityResponseMap =
          entityService.getEntitiesV2(
              opContext,
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

    return Pair.of(testInfoMap, buildTestEngine(opContext, testInfoMap));
  }

  private TestEngine buildTestEngine(
      @Nonnull OperationContext opContext, Map<Urn, TestInfo> testInfoMap) {
    final TestFetcher testFetcher;

    if (testInfoMap.isEmpty()) {
      testFetcher = new TestFetcher(entityService, entitySearchService);
    } else {
      testFetcher = new TestListFetcher(entityService, entitySearchService, testInfoMap);
    }

    TestsConfiguration singleRequestConfig =
        testsConfiguration.toBuilder()
            .cacheRefreshIntervalSecs(0) // no cache refresh during request
            .jvmShutdownHookEnabled(false) // no expectation of shutdown hook inside the request
            .build();

    return new TestEngine(
        opContext,
        singleRequestConfig,
        entityService,
        entitySearchService,
        timeseriesAspectService,
        testFetcher,
        new TestDefinitionParser(predicateEvaluator),
        queryEngine,
        predicateEvaluator,
        actionApplier,
        actionsExecutorService);
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
                      TestResults testResult = urnTestResult.getSecond();

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
    public TestFetchResult fetch(
        @Nonnull OperationContext opContext, int start, int count, String query)
        throws RemoteInvocationException, URISyntaxException {
      return new TestFetchResult(
          tests.subList(start, Math.min(tests.size(), start + count)), tests.size());
    }
  }

  @Override
  public ResponseEntity<List<TestEntityResponseV2>> create(
      HttpServletRequest request,
      List<TestEntityRequestV2> body,
      Boolean createIfNotExists,
      Boolean createEntityIfNotExists) {
    return entityApiDelegate
        .setRequest(request)
        .create(body, createIfNotExists, createEntityIfNotExists);
  }

  @Override
  public ResponseEntity<Void> delete(HttpServletRequest request, String urn) {
    return entityApiDelegate.setRequest(request).delete(urn);
  }

  @Override
  public ResponseEntity<TestEntityResponseV2> get(
      HttpServletRequest request, String urn, Boolean systemMetadata, List<String> aspects) {
    return entityApiDelegate.setRequest(request).get(urn, systemMetadata, aspects);
  }

  @Override
  public ResponseEntity<Void> head(HttpServletRequest request, String urn) {
    return entityApiDelegate.setRequest(request).head(urn);
  }

  @Override
  public ResponseEntity<ScrollTestEntityResponseV2> scroll(
      HttpServletRequest request,
      Boolean systemMetadata,
      List<String> aspects,
      Integer count,
      String scrollId,
      List<String> sort,
      SortOrder sortOrder,
      String query) {
    return entityApiDelegate
        .setRequest(request)
        .scroll(systemMetadata, aspects, count, scrollId, sort, sortOrder, query);
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
