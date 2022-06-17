package com.linkedin.metadata.boot;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.boot.steps.IngestDataPlatformInstancesStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import io.ebean.EbeanServer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class EbeanIngestDataPlatformInstancesTest extends IngestDataPlatformInstancesTest<EbeanAspectDao, EbeanAspectDao> {

  public EbeanIngestDataPlatformInstancesTest() throws EntityRegistryException {
  }

  @BeforeMethod
  public void setupTest() {
    EbeanServer server = EbeanTestUtils.createTestServer();
    _mockProducer = mock(EventProducer.class);
    EbeanAspectDao dao = new EbeanAspectDao(server);
    dao.setConnectionValidated(true);
    _aspectDao = dao;
    _migrationsDao = dao;
    _entityService = new EntityService(_aspectDao, _mockProducer, _testEntityRegistry);
    _retentionService = new EbeanRetentionService(_entityService, server, 1000);
    _entityService.setRetentionService(_retentionService);
    _step = new IngestDataPlatformInstancesStep(_entityService, _migrationsDao);
  }

  /**
   * Ideally, all tests would be in the base class, so they're reused between all implementations.
   * When that's the case - test runner will ignore this class (and its base!) so we keep this dummy test
   * to make sure this class will always be discovered.
   */
  @Test
  public void obligatoryTest() throws Exception {
    Assert.assertTrue(true);
  }
}
