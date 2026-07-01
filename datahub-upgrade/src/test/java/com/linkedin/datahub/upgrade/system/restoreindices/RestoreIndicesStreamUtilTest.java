package com.linkedin.datahub.upgrade.system.restoreindices;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestoreIndicesStreamUtilTest {

  @Mock private OperationContext mockOpContext;
  @Mock private EntityService<?> mockEntityService;
  @Mock private AspectDao mockAspectDao;
  @Mock private RetrieverContext mockRetrieverContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
  }

  /**
   * The scan must target the requested aspect and urn prefix (this is what makes each restore-index
   * step reindex the correct entities); an empty result set produces no MCLs.
   */
  @Test
  public void testScansRequestedAspectAndUrnPrefix() {
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(mockStream);
    when(mockStream.partition(anyInt())).thenReturn(Stream.empty());

    RestoreIndicesStreamUtil.reindexAspect(
        mockOpContext,
        mockEntityService,
        mockAspectDao,
        "upstreamLineage",
        "urn:li:dataset:%",
        1000,
        0,
        0,
        ChangeType.RESTATE);

    ArgumentCaptor<RestoreIndicesArgs> captor = ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao).streamAspectBatches(any(OperationContext.class), captor.capture());
    assertEquals(captor.getValue().aspectName(), "upstreamLineage");
    assertEquals(captor.getValue().urnLike(), "urn:li:dataset:%");
    assertEquals(captor.getValue().batchSize(), 1000);

    // No aspects streamed -> no MCLs emitted.
    verifyNoInteractions(mockEntityService);
  }
}
