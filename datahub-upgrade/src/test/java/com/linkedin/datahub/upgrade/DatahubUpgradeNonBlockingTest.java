package com.linkedin.datahub.upgrade;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import com.datahub.util.RecordUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCPStep;
import com.linkedin.datahub.upgrade.system.restoreindices.graph.vianodes.ReindexDataJobViaNodesCLL;
import com.linkedin.metadata.boot.kafka.MockSystemUpdateDeserializer;
import com.linkedin.metadata.boot.kafka.MockSystemUpdateSerializer;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.Topics;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Named;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    properties = {
      "BOOTSTRAP_SYSTEM_UPDATE_DATA_JOB_NODE_CLL_ENABLED=true",
      "kafka.schemaRegistry.type=INTERNAL",
      "DATAHUB_UPGRADE_HISTORY_TOPIC_NAME=" + Topics.DATAHUB_UPGRADE_HISTORY_TOPIC_NAME,
      "METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME=" + Topics.METADATA_CHANGE_LOG_VERSIONED,
    },
    args = {"-u", "SystemUpdateNonBlocking"})
public class DatahubUpgradeNonBlockingTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("systemUpdateNonBlocking")
  private SystemUpdateNonBlocking systemUpdateNonBlocking;

  @Autowired
  @Named("schemaRegistryConfig")
  private KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig;

  @Autowired
  @Named("duheKafkaEventProducer")
  private KafkaEventProducer duheKafkaEventProducer;

  @Autowired
  @Named("kafkaEventProducer")
  private KafkaEventProducer kafkaEventProducer;

  @Autowired private EntityServiceImpl entityService;

  private OperationContext opContext;

  @BeforeClass
  public void init() {
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @Test
  public void testSystemUpdateNonBlockingInit() {
    assertNotNull(systemUpdateNonBlocking);

    // Expected system update configuration and producer
    assertEquals(
        schemaRegistryConfig.getValue().getDeserializer(),
        MockSystemUpdateDeserializer.class.getName());
    assertEquals(
        schemaRegistryConfig.getValue().getSerializer(),
        MockSystemUpdateSerializer.class.getName());
    assertEquals(duheKafkaEventProducer, kafkaEventProducer);
    assertEquals(entityService.getProducer(), duheKafkaEventProducer);
  }

  @Test
  public void testReindexDataJobViaNodesCLLPagingArgs() {
    EntityService<?> mockService = mock(EntityService.class);

    AspectDao mockAspectDao = mock(AspectDao.class);
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockStream.partition(anyInt())).thenReturn(Stream.empty());
    when(mockAspectDao.streamAspectBatches(any(RestoreIndicesArgs.class))).thenReturn(mockStream);

    ReindexDataJobViaNodesCLL cllUpgrade =
        new ReindexDataJobViaNodesCLL(opContext, mockService, mockAspectDao, true, 10, 0, 0);
    SystemUpdateNonBlocking upgrade = new SystemUpdateNonBlocking(List.of(cllUpgrade), null);
    DefaultUpgradeManager manager = new DefaultUpgradeManager();
    manager.register(upgrade);
    manager.execute(
        TestOperationContexts.systemContextNoSearchAuthorization(),
        "SystemUpdateNonBlocking",
        List.of());
    verify(mockAspectDao, times(1))
        .streamAspectBatches(
            eq(
                new RestoreIndicesArgs()
                    .batchSize(10)
                    .limit(0)
                    .aspectName("dataJobInputOutput")
                    .urnBasedPagination(false)
                    .lastUrn(null)
                    .urnLike("urn:li:dataJob:%")));
  }

  @Test
  public void testReindexDataJobViaNodesCLLResumePaging() throws Exception {
    // Mock services
    EntityService<?> mockService = mock(EntityService.class);
    AspectDao mockAspectDao = mock(AspectDao.class);

    // Create test data
    EbeanAspectV2 aspect1 = createMockEbeanAspect("urn:li:dataJob:job1", "dataJobInputOutput");
    EbeanAspectV2 aspect2 = createMockEbeanAspect("urn:li:dataJob:job2", "dataJobInputOutput");
    EbeanAspectV2 aspect3 = createMockEbeanAspect("urn:li:dataJob:job3", "dataJobInputOutput");
    List<EbeanAspectV2> initialBatch = Arrays.asList(aspect1, aspect2);
    List<EbeanAspectV2> resumeBatch = Arrays.asList(aspect3);

    // Mock the stream for first batch
    PartitionedStream<EbeanAspectV2> initialStream = mock(PartitionedStream.class);
    when(initialStream.partition(anyInt())).thenReturn(Stream.of(initialBatch.stream()));

    // Mock the stream for second batch
    PartitionedStream<EbeanAspectV2> resumeStream = mock(PartitionedStream.class);
    when(resumeStream.partition(anyInt())).thenReturn(Stream.of(resumeBatch.stream()));

    // Setup the AspectDao using Answer to handle null safely
    when(mockAspectDao.streamAspectBatches(any(RestoreIndicesArgs.class)))
        .thenAnswer(
            invocation -> {
              RestoreIndicesArgs args = invocation.getArgument(0);
              if (args.lastUrn() == null) {
                return initialStream;
              } else if ("urn:li:dataJob:job2".equals(args.lastUrn())) {
                return resumeStream;
              }
              return mock(PartitionedStream.class);
            });

    // Mock successful MCL production
    when(mockService.alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Pair.of(CompletableFuture.completedFuture(null), true));

    // Create the upgrade
    ReindexDataJobViaNodesCLL cllUpgrade =
        new ReindexDataJobViaNodesCLL(opContext, mockService, mockAspectDao, true, 2, 0, 0);

    // Initial Run
    cllUpgrade.steps().get(0).executable().apply(createMockInitialUpgrade());

    // Resumed
    cllUpgrade.steps().get(0).executable().apply(createMockResumeUpgrade());

    // Use ArgumentCaptor to verify the calls
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao, times(2)).streamAspectBatches(argsCaptor.capture());

    List<RestoreIndicesArgs> capturedArgs = argsCaptor.getAllValues();

    // Verify both the initial and resume calls were made with correct arguments
    assertEquals(capturedArgs.get(0).lastUrn(), null);
    assertEquals(capturedArgs.get(0).urnBasedPagination(), false);
    assertEquals(capturedArgs.get(1).lastUrn(), "urn:li:dataJob:job2");
    assertEquals(capturedArgs.get(1).urnBasedPagination(), true);

    // Verify MCL production was called for each aspect
    verify(mockService, times(3))
        .alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testNonBlockingBootstrapMCP() {
    List<BootstrapMCPStep> mcpTemplate =
        systemUpdateNonBlocking.steps().stream()
            .filter(update -> update instanceof BootstrapMCPStep)
            .map(update -> (BootstrapMCPStep) update)
            .toList();

    assertFalse(mcpTemplate.isEmpty());
    assertTrue(
        mcpTemplate.stream().noneMatch(update -> update.getMcpTemplate().isBlocking()),
        String.format(
            "Found blocking step: %s (expected non-blocking only)",
            mcpTemplate.stream()
                .filter(update -> update.getMcpTemplate().isBlocking())
                .map(update -> update.getMcpTemplate().getName())
                .collect(Collectors.toSet())));
  }

  private UpgradeContext createMockInitialUpgrade() {
    // Mock the Upgrade instance
    Upgrade mockUpgrade = mock(Upgrade.class);

    // Configure the mock upgrade to return no previous result
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    UpgradeContext mockInitialContext = mock(UpgradeContext.class);
    when(mockInitialContext.opContext()).thenReturn(opContext);
    when(mockInitialContext.upgrade()).thenReturn(mockUpgrade);
    when(mockInitialContext.report()).thenReturn(mock(UpgradeReport.class));

    return mockInitialContext;
  }

  private UpgradeContext createMockResumeUpgrade() {
    // Mock the Upgrade instance
    Upgrade mockUpgrade = mock(Upgrade.class);
    DataHubUpgradeResult mockPrevResult = mock(DataHubUpgradeResult.class);

    // Configure the mock previous result
    when(mockPrevResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(mockPrevResult.getResult())
        .thenReturn(new StringMap(Map.of("lastUrn", "urn:li:dataJob:job2")));

    // Configure the mock upgrade to return our previous result
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(mockPrevResult));

    UpgradeContext mockResumeContext = mock(UpgradeContext.class);
    when(mockResumeContext.opContext()).thenReturn(opContext);
    when(mockResumeContext.upgrade()).thenReturn(mockUpgrade);
    when(mockResumeContext.report()).thenReturn(mock(UpgradeReport.class));

    return mockResumeContext;
  }

  private static EbeanAspectV2 createMockEbeanAspect(String urn, String aspectName) {
    Timestamp now = new Timestamp(System.currentTimeMillis());
    return new EbeanAspectV2(
        urn,
        aspectName,
        0L,
        "{}", // metadata
        now, // createdOn
        "urn:li:corpuser:testUser", // createdBy
        null, // createdFor
        RecordUtils.toJsonString(
            SystemMetadataUtils.createDefaultSystemMetadata()) // systemMetadata
        );
  }
}
