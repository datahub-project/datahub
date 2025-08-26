package com.linkedin.datahub.upgrade.system.executorpools;

import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.executorpool.*;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.key.RemoteExecutorPoolKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.paginators.ListQueuesIterable;

public class ExecutorPoolsStepTest {

  private static final String TEST_CUSTOMER_ID = "test-customer-instance";

  @Mock private OperationContext mockOpContext;
  @Mock private EntityService<?> mockEntityService;
  @Mock private SearchService mockSearchService;
  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private SqsClient mockSqsClient;

  private final Region region = Region.of("us-west2");
  private final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

  private ExecutorPoolsStep step;

  private <T extends RecordTemplate> AspectSpec createMockAspectSpec(
      Class<T> clazz, RecordDataSchema schema) {
    AspectSpec mockSpec = mock(AspectSpec.class);
    when(mockSpec.getDataTemplateClass()).thenReturn((Class<RecordTemplate>) clazz);
    when(mockSpec.getPegasusSchema()).thenReturn(schema);
    return mockSpec;
  }

  private void addExecutorPoolInfoEntry(List<MetadataChangeProposal> mcps, String queueName) {
    final Urn poolUrn =
        Urn.createFromTuple(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, queueName);
    final RemoteExecutorPoolState poolState = new RemoteExecutorPoolState();
    poolState.setStatus(RemoteExecutorPoolStatus.READY);

    final RemoteExecutorPoolInfo poolInfo = new RemoteExecutorPoolInfo();
    poolInfo.setCreatedAt(System.currentTimeMillis());
    poolInfo.setDescription("Created by upgrade job");
    poolInfo.setQueueUrl(
        String.format("https://sqs-api.aws/re-%s-%s", TEST_CUSTOMER_ID, queueName));
    poolInfo.setQueueRegion(region.toString());
    poolInfo.setIsEmbedded(false);
    poolInfo.setState(poolState);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(poolUrn);
    proposal.setEntityType(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME);
    proposal.setAspectName(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setAspect(GenericRecordUtils.serializeAspect(poolInfo));
    proposal.setSystemMetadata(createDefaultSystemMetadata());
    mcps.add(proposal);
  }

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    step =
        new ExecutorPoolsStep(
            mockOpContext,
            mockEntityService,
            mockSearchService,
            mockSqsClient,
            true,
            false,
            1000,
            TEST_CUSTOMER_ID,
            region,
            auditStamp);

    final EntitySpec mockEntitySpec = Mockito.mock(EntitySpec.class);

    final AspectSpec mockExecutorPoolInfo =
        createMockAspectSpec(RemoteExecutorPoolInfo.class, RemoteExecutorPoolInfo.dataSchema());
    Mockito.when(mockEntitySpec.getAspectSpec(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME))
        .thenReturn(mockExecutorPoolInfo);

    final AspectSpec mockExecutorPoolKey =
        createMockAspectSpec(RemoteExecutorPoolKey.class, RemoteExecutorPoolKey.dataSchema());
    Mockito.when(mockEntitySpec.getKeyAspectSpec()).thenReturn(mockExecutorPoolKey);

    final AspectSpec mockCorpuser =
        createMockAspectSpec(CorpUserInfo.class, CorpUserInfo.dataSchema());
    Mockito.when(mockEntitySpec.getAspectSpec(Constants.CORP_USER_INFO_ASPECT_NAME))
        .thenReturn(mockCorpuser);

    final EntityRegistry mockEntityRegistry = Mockito.mock(EntityRegistry.class);
    Mockito.when(mockEntityRegistry.getEntitySpec(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME))
        .thenReturn(mockEntitySpec);
    Mockito.when(mockEntityRegistry.getEntitySpec(Constants.CORP_USER_ENTITY_NAME))
        .thenReturn(mockEntitySpec);

    final AspectRetriever mockAspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(mockAspectRetriever.getEntityRegistry()).thenReturn(mockEntityRegistry);
    Mockito.when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    Mockito.when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
  }

  /** Test to verify the correct step ID is returned. */
  @Test
  public void testId() {
    assertEquals("ExecutorPools", step.id());
  }

  /**
   * Test to verify the executable function processes batches correctly and returns a success
   * result.
   */
  @Test
  public void testExecutable() {
    UpgradeContext mockContext = Mockito.mock(UpgradeContext.class);

    ListQueuesRequest listQueuesRequest =
        ListQueuesRequest.builder()
            .queueNamePrefix(String.format("re-%s", TEST_CUSTOMER_ID))
            .build();

    List<String> testQueues = List.of("remote-test", "remote-test-2", "executor_with_underscores");
    List<ListQueuesResponse> listQueues =
        new ArrayList<>(
            List.of(
                ListQueuesResponse.builder()
                    .queueUrls(
                        testQueues.stream()
                            .map(
                                item ->
                                    String.format(
                                        "https://sqs-api.aws/re-%s-%s", TEST_CUSTOMER_ID, item))
                            .collect(Collectors.toList()))
                    .build()));

    ListQueuesIterable mockQueuesIterable = Mockito.mock(ListQueuesIterable.class);
    Mockito.when(mockQueuesIterable.stream()).thenReturn(listQueues.stream());
    Mockito.when(mockSqsClient.listQueuesPaginator(listQueuesRequest))
        .thenReturn(mockQueuesIterable);

    // Simulate existence of "remote-test-2" queue
    final Urn existingUrn =
        Urn.createFromTuple(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, "remote-test-2");
    Mockito.when(
            mockEntityService.exists(
                mockOpContext,
                existingUrn,
                AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME,
                false))
        .thenReturn(true);

    step.executable().apply(mockContext);

    List<MetadataChangeProposal> test_mcps = new ArrayList<>();
    List<String> expectedQueues = List.of("remote-test", "executor_with_underscores");
    expectedQueues.stream().forEach(item -> addExecutorPoolInfoEntry(test_mcps, item));

    AspectsBatch test_batch =
        AspectsBatchImpl.builder()
            .mcps(test_mcps, auditStamp, mockRetrieverContext)
            .build(mockOpContext);

    Mockito.verify(mockEntityService)
        .ingestProposal(
            Mockito.any(),
            Mockito.argThat(new ExecutorPoolsStepMatcherTest(test_batch)),
            Mockito.same(false));
  }
}
