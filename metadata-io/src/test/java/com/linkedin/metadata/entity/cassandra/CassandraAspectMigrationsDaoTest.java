package com.linkedin.metadata.entity.cassandra;

import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.CassandraTestUtils;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.AspectMigrationsDaoTest;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.service.UpdateIndicesService;
import java.util.List;
import org.testcontainers.cassandra.CassandraContainer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CassandraAspectMigrationsDaoTest extends AspectMigrationsDaoTest<CassandraAspectDao> {

  private CassandraContainer _cassandraContainer;
  private CqlSession _currentSession;

  public CassandraAspectMigrationsDaoTest() throws EntityRegistryException {}

  @BeforeClass
  public void setupContainer() {
    _cassandraContainer = CassandraTestUtils.setupContainer();
  }

  @AfterMethod
  public void cleanup() {
    CassandraTestUtils.closeSession(_currentSession);
    _currentSession = null;
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
    _currentSession = CassandraTestUtils.createTestSession(_cassandraContainer);

    CassandraAspectDao dao = new CassandraAspectDao(_currentSession, List.of(), null);
    dao.setConnectionValidated(true);
    _mockProducer = mock(EventProducer.class);
    _mockUpdateIndicesService = mock(UpdateIndicesService.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    _entityServiceImpl = new EntityServiceImpl(dao, _mockProducer, true, preProcessHooks, true);
    _entityServiceImpl.setUpdateIndicesService(_mockUpdateIndicesService);
    _retentionService = new CassandraRetentionService(_entityServiceImpl, _currentSession, 1000);
    _entityServiceImpl.setRetentionService(_retentionService);

    _migrationsDao = dao;
  }

  /**
   * Ideally, all tests would be in the base class, so they're reused between all implementations.
   * When that's the case - test runner will ignore this class (and its base!) so we keep this dummy
   * test to make sure this class will always be discovered.
   */
  @Test
  public void obligatoryTest() throws AssertionError {
    Assert.assertTrue(true);
  }
}
