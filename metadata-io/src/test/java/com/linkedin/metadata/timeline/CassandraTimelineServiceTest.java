package com.linkedin.metadata.timeline;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.CassandraTestUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import org.testcontainers.containers.CassandraContainer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

/**
 * A class that knows how to configure {@link TimelineServiceTest} to run integration tests against a Cassandra database.
 *
 * This class also contains all the test methods where realities of an underlying storage leak into the
 * {@link TimelineServiceImpl} in the form of subtle behavior differences. Ideally that should never happen, and it'd be
 * great to address captured differences.
 */
public class CassandraTimelineServiceTest extends TimelineServiceTest<CassandraAspectDao> {

  private CassandraContainer _cassandraContainer;

  public CassandraTimelineServiceTest() throws EntityRegistryException {
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
    _aspectDao = new CassandraAspectDao(session);
    _aspectDao.setConnectionValidated(true);
    _entityTimelineService = new TimelineServiceImpl(_aspectDao, _testEntityRegistry);
    _mockProducer = mock(EventProducer.class);
    _entityService = new EntityService(_aspectDao, _mockProducer, _testEntityRegistry);
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
