package com.linkedin.metadata.boot;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.CassandraTestUtils;
import com.linkedin.metadata.boot.steps.IngestDataPlatformInstancesStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.cassandra.CassandraRetentionService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import org.testcontainers.containers.CassandraContainer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class CassandraIngestDataPlatformInstancesTest extends IngestDataPlatformInstancesTest<CassandraAspectDao, CassandraAspectDao> {

  private CassandraContainer _cassandraContainer;

  public CassandraIngestDataPlatformInstancesTest() throws EntityRegistryException {
  }

  @BeforeClass
  public void setupContainer() {
    _cassandraContainer = CassandraTestUtils.setupContainer();
  }

  @AfterClass
  public void tearDown() {
    _cassandraContainer.stop();
  }

  @BeforeMethod
  public void setupTest() {
    CassandraTestUtils.purgeData(_cassandraContainer);
    configureComponents();
  }

  private void configureComponents() {
    CqlSession session = CassandraTestUtils.createTestSession(_cassandraContainer);
    CassandraAspectDao dao = new CassandraAspectDao(session);
    dao.setConnectionValidated(true);
    _aspectDao = dao;
    _migrationsDao = dao;
    _mockProducer = mock(EventProducer.class);
    _entityService = new EntityService(_aspectDao, _mockProducer, _testEntityRegistry);
    _retentionService = new CassandraRetentionService(_entityService, session, 1000);
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
