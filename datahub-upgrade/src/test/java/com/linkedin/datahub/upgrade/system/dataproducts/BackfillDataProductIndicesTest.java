package com.linkedin.datahub.upgrade.system.dataproducts;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BackfillDataProductIndicesTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final String DP_URN_1 = "urn:li:dataProduct:product-1";
  private static final String DP_URN_2 = "urn:li:dataProduct:product-2";

  private EntityService<?> entityService;
  private AspectDao aspectDao;
  private UpgradeContext upgradeContext;
  private Upgrade upgrade;
  private UpgradeReport upgradeReport;

  @BeforeMethod
  public void setup() {
    entityService = mock(EntityService.class);
    aspectDao = mock(AspectDao.class);
    upgradeContext = mock(UpgradeContext.class);
    upgrade = mock(Upgrade.class);
    upgradeReport = mock(UpgradeReport.class);
    when(upgradeContext.upgrade()).thenReturn(upgrade);
    when(upgradeContext.report()).thenReturn(upgradeReport);
    when(upgradeContext.opContext()).thenReturn(OP_CONTEXT);
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
  }

  @Test
  public void testDisabledHasNoSteps() {
    BackfillDataProductIndices upgradeJob =
        new BackfillDataProductIndices(OP_CONTEXT, entityService, aspectDao, false, 100, 0, 0);
    assertTrue(upgradeJob.steps().isEmpty());
  }

  @Test
  public void testEnabledStepTargetsDataProductProperties() {
    BackfillDataProductIndices upgradeJob =
        new BackfillDataProductIndices(OP_CONTEXT, entityService, aspectDao, true, 100, 0, 0);
    assertEquals(upgradeJob.steps().size(), 1);

    BackfillDataProductIndicesStep step =
        (BackfillDataProductIndicesStep) upgradeJob.steps().get(0);
    assertEquals(step.id(), "BackfillDataProductIndices-v1");
    assertEquals(step.getAspectName(), DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    assertEquals(step.getUrnLike(), "urn:li:dataProduct:%");
    assertTrue(step.isOptional());
  }

  @Test
  public void testSkipWhenPreviousRunSucceeded() {
    BackfillDataProductIndicesStep step =
        new BackfillDataProductIndicesStep(OP_CONTEXT, entityService, aspectDao, 100, 0, 0);

    DataHubUpgradeResult previousResult =
        new DataHubUpgradeResult().setState(DataHubUpgradeState.SUCCEEDED);
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(previousResult));

    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testDoesNotSkipWhenPreviousRunFailed() {
    BackfillDataProductIndicesStep step =
        new BackfillDataProductIndicesStep(OP_CONTEXT, entityService, aspectDao, 100, 0, 0);

    DataHubUpgradeResult previousResult =
        new DataHubUpgradeResult().setState(DataHubUpgradeState.FAILED);
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(previousResult));

    assertFalse(step.skip(upgradeContext));
  }

  @Test
  public void testExecuteStreamsAspectsAndMarksSucceeded() {
    BackfillDataProductIndicesStep step =
        new BackfillDataProductIndicesStep(OP_CONTEXT, entityService, aspectDao, 2, 0, 0);

    EbeanAspectV2 aspect1 = createMockEbeanAspect(DP_URN_1, DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    EbeanAspectV2 aspect2 = createMockEbeanAspect(DP_URN_2, DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    List<EbeanAspectV2> batch = Arrays.asList(aspect1, aspect2);

    PartitionedStream<EbeanAspectV2> partitionedStream = mock(PartitionedStream.class);
    when(partitionedStream.partition(anyInt())).thenReturn(Stream.of(batch.stream()));

    when(aspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class), any()))
        .thenAnswer(
            invocation ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        invocation.getArgument(2))
                    .apply(partitionedStream));

    when(entityService.alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Pair.of(CompletableFuture.completedFuture(null), true));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(eq(OP_CONTEXT), argsCaptor.capture(), any());
    RestoreIndicesArgs args = argsCaptor.getValue();
    assertEquals(args.aspectName, DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    assertEquals(args.urnLike, "urn:li:dataProduct:%");
    assertEquals(args.batchSize, Integer.valueOf(2));

    verify(entityService, times(2))
        .alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    // Progress marker written while streaming; final SUCCEEDED marker via BootstrapStep
    verify(upgrade, atLeastOnce())
        .setUpgradeResult(
            any(), any(), eq(entityService), eq(DataHubUpgradeState.IN_PROGRESS), any());
  }

  private static EbeanAspectV2 createMockEbeanAspect(String urn, String aspectName) {
    Timestamp now = new Timestamp(System.currentTimeMillis());
    return new EbeanAspectV2(
        urn,
        aspectName,
        0L,
        "{}",
        now,
        "urn:li:corpuser:testUser",
        null,
        RecordUtils.toJsonString(SystemMetadataUtils.createDefaultSystemMetadata()));
  }
}
