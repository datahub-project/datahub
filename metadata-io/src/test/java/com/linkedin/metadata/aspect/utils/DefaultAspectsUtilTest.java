package com.linkedin.metadata.aspect.utils;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.linkedin.common.BrowsePaths;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.PlatformType;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class DefaultAspectsUtilTest {

  private static final Urn HDFS_PLATFORM_URN = UrnUtils.getUrn("urn:li:dataPlatform:hdfs");
  private static final Urn HDFS_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,a/b/c,PROD)");

  private Database server;

  public DefaultAspectsUtilTest() {}

  @Test
  public void testGetDataPlatformInfoUsesCachingAspectRetrieverDelimiter() {
    DataPlatformInfo platformInfo =
        new DataPlatformInfo()
            .setName("hdfs")
            .setType(PlatformType.FILE_SYSTEM)
            .setDatasetNameDelimiter("/");
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);
    when(retriever.getLatestAspectObject(
            any(), eq(HDFS_PLATFORM_URN), eq(DATA_PLATFORM_INFO_ASPECT_NAME)))
        .thenReturn(new Aspect(platformInfo.data()));

    BrowsePaths browsePaths =
        DefaultAspectsUtil.buildDefaultBrowsePath(mockOpContext(retriever), HDFS_DATASET_URN);

    Assert.assertEquals(browsePaths.getPaths().get(0), "/prod/hdfs/a/b");
    verify(retriever)
        .getLatestAspectObject(any(), eq(HDFS_PLATFORM_URN), eq(DATA_PLATFORM_INFO_ASPECT_NAME));
  }

  @Test
  public void testGetDataPlatformInfoMissingFallsBackToDefaultDelimiter() {
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);
    when(retriever.getLatestAspectObject(
            any(), eq(HDFS_PLATFORM_URN), eq(DATA_PLATFORM_INFO_ASPECT_NAME)))
        .thenReturn(null);

    BrowsePaths browsePaths =
        DefaultAspectsUtil.buildDefaultBrowsePath(mockOpContext(retriever), HDFS_DATASET_URN);

    // Name has no '.', so default delimiter yields no dataset name path segments.
    Assert.assertEquals(browsePaths.getPaths().get(0), "/prod/hdfs");
    verify(retriever)
        .getLatestAspectObject(any(), eq(HDFS_PLATFORM_URN), eq(DATA_PLATFORM_INFO_ASPECT_NAME));
  }

  @Test
  public void testGetDataPlatformInfoExceptionFallsBackToDefaultDelimiter() {
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);
    when(retriever.getLatestAspectObject(
            any(), eq(HDFS_PLATFORM_URN), eq(DATA_PLATFORM_INFO_ASPECT_NAME)))
        .thenThrow(new RuntimeException("cache miss failure"));

    BrowsePaths browsePaths =
        DefaultAspectsUtil.buildDefaultBrowsePath(mockOpContext(retriever), HDFS_DATASET_URN);

    Assert.assertEquals(browsePaths.getPaths().get(0), "/prod/hdfs");
  }

  private OperationContext mockOpContext(CachingAspectRetriever retriever) {
    OperationContext opContext = mock(OperationContext.class);
    RetrieverContext retrieverContext = mock(RetrieverContext.class);
    when(opContext.getEntityRegistry()).thenReturn(new TestEntityRegistry());
    when(opContext.getRetrieverContext()).thenReturn(retrieverContext);
    when(retrieverContext.getCachingAspectRetriever()).thenReturn(retriever);
    return opContext;
  }

  @Test
  public void testAdditionalChanges() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    server = EbeanTestUtils.createTestServer(DefaultAspectsUtilTest.class.getSimpleName());
    EbeanAspectDao aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            null,
            List.of(),
            null,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server));
    aspectDao.setConnectionValidated(true);
    EventProducer mockProducer = mock(EventProducer.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    EntityServiceImpl entityServiceImpl =
        new EntityServiceImpl(
            aspectDao,
            mockProducer,
            preProcessHooks,
            new EntityServiceConfiguration().setAlwaysEmitChangeLog(true).setEnableBrowseV2(false),
            mock(MetricUtils.class));

    MetadataChangeProposal proposal1 =
        new DatasetPropertiesPatchBuilder()
            .urn(new DatasetUrn(new DataPlatformUrn("platform"), "name", FabricType.PROD))
            .setDescription("something")
            .setName("name")
            .addCustomProperty("prop1", "propVal1")
            .addCustomProperty("prop2", "propVal2")
            .build();

    Assert.assertEquals(proposal1.getChangeType(), ChangeType.PATCH);

    List<MetadataChangeProposal> proposalList =
        DefaultAspectsUtil.getAdditionalChanges(
                opContext,
                AspectsBatchImpl.builder()
                    .mcps(
                        List.of(proposal1),
                        AuditStampUtils.createDefaultAuditStamp(),
                        opContext.getRetrieverContext())
                    .build(opContext)
                    .getMCPItems(),
                entityServiceImpl,
                false)
            .stream()
            .map(MCPItem::getMetadataChangeProposal)
            .collect(Collectors.toList());
    // proposals for key aspect, browsePath, browsePathV2, dataPlatformInstance
    Assert.assertEquals(proposalList.size(), 4);
    Assert.assertEquals(
        proposalList.stream()
            .map(MetadataChangeProposal::getChangeType)
            .collect(Collectors.toList()),
        List.of(ChangeType.CREATE, ChangeType.CREATE, ChangeType.CREATE, ChangeType.CREATE));
  }

  @AfterMethod
  public void cleanup() {
    EbeanTestUtils.shutdownDatabase(server);
  }
}
