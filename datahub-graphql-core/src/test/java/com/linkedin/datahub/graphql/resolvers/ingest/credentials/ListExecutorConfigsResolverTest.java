package com.linkedin.datahub.graphql.resolvers.ingest.credentials;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ExecutorConfigs;
import com.linkedin.datahub.graphql.generated.ListExecutorConfigsInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorpool.*;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.config.ExecutorConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class ListExecutorConfigsResolverTest {

  private static final ListExecutorConfigsInput TEST_INPUT =
      new ListExecutorConfigsInput(List.of("remote-test"));

  private EntityClient mockEntityClient;
  private StsClient mockStsClient;
  private ExecutorConfiguration mockExecutorConfiguration;
  private Map<Urn, EntityResponse> sharedExamples;
  private ListExecutorConfigsResolver resolver;

  private static final long TEST_STS_EXPIRATION_EPOCH = 1678886400L;
  private static final String TEST_STS_ACCESS_KEY = "test-access-key";
  private static final String TEST_STS_ACCESS_KEY_SECRET = "test-access-key-secret";
  private static final String TEST_STS_SESSION_TOKEN = "test-session-token";
  private static final String TEST_STS_ROLE_ARN = "arn:aws:iam::1234567890:role/test-role";

  public static void assertExecutorConfigEquals(ExecutorConfigs expected, ExecutorConfigs actual) {
    assertEquals(expected.getRegion(), actual.getRegion());
    assertEquals(expected.getExecutorId(), actual.getExecutorId());
    assertEquals(expected.getQueueUrl(), actual.getQueueUrl());
    assertEquals(expected.getAccessKeyId(), actual.getAccessKeyId());
    assertEquals(expected.getSecretKeyId(), actual.getSecretKeyId());
    assertEquals(expected.getSessionToken(), actual.getSessionToken());
    assertEquals(expected.getExpiration(), actual.getExpiration());
  }

  public static void putTestAspect(
      Map<Urn, EntityResponse> out, Urn urn, RemoteExecutorPoolInfo aspect) {
    EntityResponse response =
        new EntityResponse()
            .setEntityName(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(aspect.data())))));
    out.put(urn, response);
  }

  public static RemoteExecutorPoolInfo buildPoolAspect(
      String region, String url, boolean isEmbedded, RemoteExecutorPoolStatus status) {
    final RemoteExecutorPoolState poolState = new RemoteExecutorPoolState();
    poolState.setStatus(status);

    final RemoteExecutorPoolInfo poolInfo = new RemoteExecutorPoolInfo();
    poolInfo.setCreatedAt(0);
    poolInfo.setQueueUrl(url);
    poolInfo.setQueueRegion(region);
    poolInfo.setIsEmbedded(isEmbedded);
    poolInfo.setState(poolState);

    return poolInfo;
  }

  public static Map<Urn, EntityResponse> executorPoolExamples() {
    Map<Urn, EntityResponse> testPools = new HashMap<>();
    putTestAspect(
        testPools,
        Urn.createFromTuple(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, "default"),
        buildPoolAspect(
            "us-west-1", "https://sqs.aws/default-queue", true, RemoteExecutorPoolStatus.READY));
    putTestAspect(
        testPools,
        Urn.createFromTuple(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, "remote-test"),
        buildPoolAspect(
            "us-east-1",
            "https://sqs.aws/remote-test-queue",
            false,
            RemoteExecutorPoolStatus.READY));
    putTestAspect(
        testPools,
        Urn.createFromTuple(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, "remote-test-2"),
        buildPoolAspect(
            "us-south-1",
            "https://sqs.aws/remote-test-2-queue",
            false,
            RemoteExecutorPoolStatus.PROVISIONING_FAILED));
    putTestAspect(
        testPools,
        Urn.createFromTuple(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, "remote-test-3"),
        buildPoolAspect(
            "us-north-1",
            "https://sqs.aws/remote-test-3-queue",
            false,
            RemoteExecutorPoolStatus.READY));
    return testPools;
  }

  @BeforeMethod
  public void before() throws Exception {
    /** Mock entityClient */
    mockEntityClient = Mockito.mock(EntityClient.class);

    // Populate shared examples
    sharedExamples = executorPoolExamples();

    Mockito.when(
            mockEntityClient.search(
                any(),
                Mockito.eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(0),
                Mockito.eq(100)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(sharedExamples.size())
                .setEntities(
                    new SearchEntityArray(
                        sharedExamples.keySet().stream()
                            .map(item -> new SearchEntity().setEntity(item))
                            .collect(ImmutableSet.toImmutableSet()))));

    Mockito.when(
            mockEntityClient.batchGetV2(
                any(),
                Mockito.eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
                Mockito.eq(
                    new HashSet<>(
                        sharedExamples.keySet().stream().collect(ImmutableSet.toImmutableSet()))),
                Mockito.eq(ImmutableSet.of(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME))))
        .thenReturn(sharedExamples);

    // Mock executor configuration
    mockExecutorConfiguration = new ExecutorConfiguration();
    mockExecutorConfiguration.executorRoleArn = TEST_STS_ROLE_ARN;

    /** Mock stsClient */
    mockStsClient = Mockito.mock(StsClient.class);
    Mockito.when(mockStsClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId(TEST_STS_ACCESS_KEY)
                        .secretAccessKey(TEST_STS_ACCESS_KEY_SECRET)
                        .sessionToken(TEST_STS_SESSION_TOKEN)
                        .expiration(Instant.ofEpochSecond(TEST_STS_EXPIRATION_EPOCH))
                        .build())
                .build());

    // Create resolver
    resolver =
        new ListExecutorConfigsResolver(mockEntityClient, mockStsClient, mockExecutorConfiguration);
  }

  @Test
  public void testGetLegacy() throws Exception {

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(null);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    List<ExecutorConfigs> executorConfigs = resolver.get(mockEnv).get().getExecutorConfigs();

    // Assertions
    // Embedded and non-ready pools are not returned
    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 2);
    assertEquals(executorConfigs.size(), 2);

    // Validate response items
    assertExecutorConfigEquals(
        executorConfigs.get(0),
        new ExecutorConfigs(
            "remote-test",
            "us-east-1",
            "https://sqs.aws/remote-test-queue",
            "test-access-key",
            "test-access-key-secret",
            "test-session-token",
            "2023-03-15T13:20:00Z"));
    assertExecutorConfigEquals(
        executorConfigs.get(1),
        new ExecutorConfigs(
            "remote-test-3",
            "us-north-1",
            "https://sqs.aws/remote-test-3-queue",
            "test-access-key",
            "test-access-key-secret",
            "test-session-token",
            "2023-03-15T13:20:00Z"));
  }

  @Test
  public void testGetParameterized() throws Exception {
    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(new ListExecutorConfigsInput(List.of("remote-test")));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ConjunctiveCriterionArray conjunctiveCriteria = new ConjunctiveCriterionArray();
    final CriterionArray executorIdsAndArray = new CriterionArray();
    executorIdsAndArray.add(
        buildCriterion(
            "urn",
            Condition.EQUAL,
            List.of(
                String.format(
                    "urn:li:%s:remote-test", AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME))));
    conjunctiveCriteria.add(new ConjunctiveCriterion().setAnd(executorIdsAndArray));
    Filter testFilter = new Filter().setOr(conjunctiveCriteria);

    List<ExecutorConfigs> executorConfigs = resolver.get(mockEnv).get().getExecutorConfigs();
    Mockito.verify(mockEntityClient)
        .search(
            Mockito.any(),
            Mockito.eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.eq(testFilter),
            Mockito.eq(null),
            Mockito.eq(0),
            Mockito.eq(100));
  }
}
