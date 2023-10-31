package io.datahubproject.openapi.metadatatests.delegates;

import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.test.TestInfo;
import com.linkedin.metadata.test.TestFetcher;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestMode;
import com.linkedin.test.TestStatus;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.delegates.EntityApiDelegateImpl;
import io.datahubproject.openapi.generated.MetadataTestResultV1;
import io.datahubproject.openapi.generated.MetadataTestResultV1Entities;
import io.datahubproject.openapi.generated.ScrollTestEntityResponseV2;
import io.datahubproject.openapi.generated.SortOrder;
import io.datahubproject.openapi.generated.TestEntityRequestV2;
import io.datahubproject.openapi.generated.TestEntityResponseV2;
import io.datahubproject.openapi.generated.TestResultType;
import io.datahubproject.openapi.metadatatests.generated.controller.MetadataTestApiDelegate;
import org.springframework.http.ResponseEntity;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class MetadataTestsDelegateImpl implements MetadataTestApiDelegate {

    final private EntityRegistry entityRegistry;
    final private EntityService entityService;
    final private EntitySearchService entitySearchService;
    final private EntityApiDelegateImpl<TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2> entityApiDelegate;
    final private AuthorizerChain authorizationChain;

    final private boolean restApiAuthorizationEnabled;

    final private QueryEngine queryEngine;
    final private ActionApplier actionApplier;

    final private PredicateEvaluator predicateEvaluator;

    public MetadataTestsDelegateImpl(EntityService entityService, EntitySearchService entitySearchService,
                                     EntityApiDelegateImpl<TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2> entityApiDelegate,
                                     boolean restApiAuthorizationEnabled, AuthorizerChain authorizationChain,
                                     QueryEngine queryEngine, ActionApplier actionApplier, PredicateEvaluator predicateEvaluator) {
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

    public MetadataTestsDelegateImpl(EntityService entityService, EntitySearchService entitySearchService,
                                     EntityApiDelegateImpl<TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2> entityApiDelegate,
                                     boolean restApiAuthorizationEnabled, AuthorizerChain authorizationChain,
                                     QueryEngine queryEngine, ActionApplier actionApplier) {
        this(entityService, entitySearchService, entityApiDelegate, restApiAuthorizationEnabled, authorizationChain,
                queryEngine, actionApplier, PredicateEvaluator.getInstance());
    }

    @Override
    public ResponseEntity<MetadataTestResultV1> executeTest(String testUrnStr, List<String> body, Boolean evaluateOnly) {
        Optional<MetadataTestResultV1> result;

        try {
            Urn testUrn = Urn.createFromString(testUrnStr);

            result = getTestEngine(testUrn).map(testInfoEngine -> {
                TestInfo info = testInfoEngine.getFirst();
                TestEngine engine = testInfoEngine.getSecond();

                List<Urn> targetUrns = body.stream().map(urnStr -> {
                    try {
                        return Urn.createFromString(urnStr);
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());

                Map<Urn, com.linkedin.test.TestResults> testResultsMap = engine.batchEvaluateTestsForEntities(
                        targetUrns,
                        Optional.ofNullable(evaluateOnly).orElse(true) ? TestEngine.EvaluationMode.EVALUATE_ONLY
                                : TestEngine.EvaluationMode.DEFAULT
                );

                List<Pair<Urn, com.linkedin.test.TestResults>> testResultPairs = targetUrns.stream()
                        .map(targetUrn -> Pair.of(targetUrn, testResultsMap.get(targetUrn)))
                        .collect(Collectors.toList());

                return toTestResult(testUrn, info, testResultPairs);
            });
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return ResponseEntity.of(result);
    }

    private Optional<Pair<TestInfo, TestEngine>> getTestEngine(Urn testUrn) throws URISyntaxException {
        Map<Urn, EntityResponse> entityResponseMap = entityService.getEntitiesV2(
                Constants.TEST_ENTITY_NAME,
                Set.of(testUrn),
                Set.of(Constants.TEST_INFO_ASPECT_NAME, Constants.STATUS_ASPECT_NAME)
        );

        boolean softDeleted = optionalEnvelopedAspect(entityResponseMap, testUrn, Constants.STATUS_ASPECT_NAME)
                .flatMap(MetadataTestsDelegateImpl::optionalDataMap)
                .map(Status::new)
                .map(Status::isRemoved)
                .orElse(false);

        if (!softDeleted) {
            return optionalEnvelopedAspect(entityResponseMap, testUrn, Constants.TEST_INFO_ASPECT_NAME)
                    .flatMap(MetadataTestsDelegateImpl::optionalDataMap)
                    .map(TestInfo::new)
                    .filter(testInfo -> TestMode.ACTIVE == Optional.ofNullable(testInfo.getStatus()).map(TestStatus::getMode).orElse(TestMode.ACTIVE))
                    .map(testInfo -> buildTestEngine(testUrn, testInfo));
        }

        return Optional.empty();
    }

    private Pair<TestInfo, TestEngine> buildTestEngine(Urn testUrn, TestInfo testInfo) {
        return Pair.of(testInfo, new TestEngine(
                entityService,
                new SingleTestFetcher(entityService, entitySearchService, testUrn, testInfo),
                new TestDefinitionParser(predicateEvaluator),
                queryEngine,
                predicateEvaluator,
                actionApplier,
                0,
                0
        ));
    }

    private static MetadataTestResultV1 toTestResult(Urn testUrn, TestInfo testInfo, List<Pair<Urn, com.linkedin.test.TestResults>> testResults) {
        return MetadataTestResultV1.builder()
                .test(testUrn.toString())
                .testName(testInfo.hasName() ? testInfo.getName() : null)
                .entities(testResults.stream().map(urnTestResult -> {
                    Urn entityUrn = urnTestResult.getFirst();
                    com.linkedin.test.TestResults testResult = urnTestResult.getSecond();

                    MetadataTestResultV1Entities.MetadataTestResultV1EntitiesBuilder builder = MetadataTestResultV1Entities.builder()
                            .entity(entityUrn.toString());

                    if (testResult != null) {
                        boolean passing = testResult.hasPassing() && !testResult.getPassing().isEmpty();
                        boolean failing = testResult.hasFailing() && !testResult.getFailing().isEmpty();

                        if (passing || failing) {
                            builder.type(passing ? TestResultType.valueOf(testResult.getPassing().get(0).getType().toString())
                                    : TestResultType.valueOf(testResult.getFailing().get(0).getType().toString()));
                        }
                    }

                    return builder.build();
                }).collect(Collectors.toList()))
                .build();
    }

    public static class SingleTestFetcher extends TestFetcher {
        final private TestFetchResult result;
        public SingleTestFetcher(EntityService entityService, EntitySearchService entitySearchService,
                                 Urn testUrn, TestInfo testInfo) {
            super(entityService, entitySearchService);
            this.result = new TestFetchResult(List.of(new Test(testUrn, testInfo)), 1);
        }
        @Override
        public TestFetchResult fetch(int start, int count, String query) throws RemoteInvocationException, URISyntaxException {
            return result;
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
    public ResponseEntity<TestEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return entityApiDelegate.get(urn, systemMetadata, aspects);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return entityApiDelegate.head(urn);
    }

    @Override
    public ResponseEntity<ScrollTestEntityResponseV2> scroll(Boolean systemMetadata, List<String> aspects, Integer count,
                                                             String scrollId, List<String> sort, SortOrder sortOrder, String query) {
        return entityApiDelegate.scroll(systemMetadata, aspects, count, scrollId, sort, sortOrder, query);
    }

    private static Optional<EnvelopedAspect> optionalEnvelopedAspect(Map<Urn, EntityResponse> responseMap, Urn urn, String aspectName) {
        return Optional.ofNullable(responseMap.get(urn))
                .flatMap(entityResponse -> Optional.ofNullable(entityResponse.getAspects().get(aspectName)));
    }

    private static Optional<DataMap> optionalDataMap(EnvelopedAspect envelopedAspect) {
        return Optional.ofNullable(envelopedAspect).map(envAspect -> envAspect.getValue().data());
    }
}
