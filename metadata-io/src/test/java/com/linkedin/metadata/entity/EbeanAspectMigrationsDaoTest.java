package com.linkedin.metadata.entity;

import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.service.UpdateIndicesService;
import io.ebean.Database;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class EbeanAspectMigrationsDaoTest extends AspectMigrationsDaoTest<EbeanAspectDao> {

  public EbeanAspectMigrationsDaoTest() throws EntityRegistryException {
  }

  @BeforeMethod
  public void setupTest() {
    Database server = EbeanTestUtils.createTestServer();
    _mockProducer = mock(EventProducer.class);
    EbeanAspectDao dao = new EbeanAspectDao(server);
    dao.setConnectionValidated(true);
    _mockUpdateIndicesService = mock(UpdateIndicesService.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    _entityServiceImpl = new EntityServiceImpl(dao, _mockProducer, _testEntityRegistry, true,
        _mockUpdateIndicesService, preProcessHooks);
    _retentionService = new EbeanRetentionService(_entityServiceImpl, server, 1000);
    _entityServiceImpl.setRetentionService(_retentionService);

    _migrationsDao = dao;
  }

  /**
   * Ideally, all tests would be in the base class, so they're reused between all implementations.
   * When that's the case - test runner will ignore this class (and its base!) so we keep this dummy test
   * to make sure this class will always be discovered.
   */
  @Test
  public void obligatoryTest() throws AssertionError {
    Assert.assertTrue(true);
  }
}
