package com.linkedin.metadata.aspect.utils;

import static org.mockito.Mockito.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultAspectsUtilTest {

  public DefaultAspectsUtilTest() {}

  @Test
  public void testAdditionalChanges() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Database server = EbeanTestUtils.createTestServer(DefaultAspectsUtilTest.class.getSimpleName());
    EbeanAspectDao aspectDao = new EbeanAspectDao(server, EbeanConfiguration.testDefault);
    aspectDao.setConnectionValidated(true);
    EventProducer mockProducer = mock(EventProducer.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    EntityServiceImpl entityServiceImpl =
        new EntityServiceImpl(aspectDao, mockProducer, true, preProcessHooks, false);

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
                    .mcps(List.of(proposal1), new AuditStamp(), opContext.getRetrieverContext())
                    .build()
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
}
