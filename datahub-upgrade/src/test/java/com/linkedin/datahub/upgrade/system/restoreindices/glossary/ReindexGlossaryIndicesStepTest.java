package com.linkedin.datahub.upgrade.system.restoreindices.glossary;

import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Guards the migration of the legacy search-index-driven glossary reindex to the AbstractMCLStep
 * DB-streaming approach: verifies each split step scans the correct aspect and urn prefix so the
 * set of glossary entities reindexed matches the original per-aspect coverage.
 */
public class ReindexGlossaryIndicesStepTest {

  @Mock private OperationContext mockOpContext;
  @Mock private EntityService<?> mockEntityService;
  @Mock private AspectDao mockAspectDao;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testTermStepScansTermInfoByTermUrn() {
    RestoreIndicesArgs args =
        captureScanArgs(
            new ReindexGlossaryTermInfoStep(
                mockOpContext, mockEntityService, mockAspectDao, 10, 0, 100));
    assertEquals(args.aspectName(), GLOSSARY_TERM_INFO_ASPECT_NAME);
    assertEquals(args.urnLike(), "urn:li:glossaryTerm:%");
  }

  @Test
  public void testNodeStepScansNodeInfoByNodeUrn() {
    RestoreIndicesArgs args =
        captureScanArgs(
            new ReindexGlossaryNodeInfoStep(
                mockOpContext, mockEntityService, mockAspectDao, 10, 0, 100));
    assertEquals(args.aspectName(), GLOSSARY_NODE_INFO_ASPECT_NAME);
    assertEquals(args.urnLike(), "urn:li:glossaryNode:%");
  }

  /** Runs the step's executable and returns the args it passed to the aspect scan. */
  private RestoreIndicesArgs captureScanArgs(AbstractMCLStep step) {
    UpgradeContext mockContext = mock(UpgradeContext.class);
    when(mockContext.opContext()).thenReturn(mockOpContext);
    when(mockContext.report()).thenReturn(mock(UpgradeReport.class));

    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockUpgrade.getUpgradeResult(any(), any(), any()))
        .thenReturn(Optional.<DataHubUpgradeResult>empty());

    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(mockStream);
    when(mockStream.partition(anyInt())).thenReturn(Stream.empty());

    step.executable().apply(mockContext);

    ArgumentCaptor<RestoreIndicesArgs> captor = ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao).streamAspectBatches(any(OperationContext.class), captor.capture());
    return captor.getValue();
  }
}
