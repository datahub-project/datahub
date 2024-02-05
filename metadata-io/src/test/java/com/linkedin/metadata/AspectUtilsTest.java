package com.linkedin.metadata;

import static org.mockito.Mockito.*;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.ebean.Database;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AspectUtilsTest {

  protected final EntityRegistry _snapshotEntityRegistry = new TestEntityRegistry();
  protected final EntityRegistry _configEntityRegistry =
      new ConfigEntityRegistry(
          Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
  protected final EntityRegistry _testEntityRegistry =
      new MergedEntityRegistry(_snapshotEntityRegistry).apply(_configEntityRegistry);

  public AspectUtilsTest() throws EntityRegistryException {}

  @Test
  public void testAdditionalChanges() {
    Database server = EbeanTestUtils.createTestServer(AspectUtilsTest.class.getSimpleName());
    EbeanAspectDao aspectDao = new EbeanAspectDao(server);
    aspectDao.setConnectionValidated(true);
    EventProducer mockProducer = mock(EventProducer.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    EntityServiceImpl entityServiceImpl =
        new EntityServiceImpl(
            aspectDao, mockProducer, _testEntityRegistry, true, null, preProcessHooks);

    MetadataChangeProposal proposal1 = new MetadataChangeProposal();
    proposal1.setEntityUrn(
        new DatasetUrn(new DataPlatformUrn("platform"), "name", FabricType.PROD));
    proposal1.setAspectName("datasetProperties");
    DatasetProperties datasetProperties = new DatasetProperties().setName("name");
    proposal1.setAspect(GenericRecordUtils.serializeAspect(datasetProperties));
    proposal1.setEntityType("dataset");
    proposal1.setChangeType(ChangeType.PATCH);

    List<MetadataChangeProposal> proposalList =
        AspectUtils.getAdditionalChanges(proposal1, entityServiceImpl);
    // proposals for key aspect, browsePath, browsePathV2, dataPlatformInstance
    Assert.assertEquals(proposalList.size(), 4);
    Assert.assertEquals(proposalList.get(0).getChangeType(), ChangeType.UPSERT);
  }
}
