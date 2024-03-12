package com.linkedin.metadata.timeline;

import static org.mockito.Mockito.mock;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import io.ebean.Database;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A class that knows how to configure {@link TimelineServiceTest} to run integration tests against
 * a relational database.
 *
 * <p>This class also contains all the test methods where realities of an underlying storage leak
 * into the {@link TimelineServiceImpl} in the form of subtle behavior differences. Ideally that
 * should never happen, and it'd be great to address captured differences.
 */
public class EbeanTimelineServiceTest extends TimelineServiceTest<EbeanAspectDao> {

  public EbeanTimelineServiceTest() throws EntityRegistryException {}

  @BeforeMethod
  public void setupTest() {
    Database server =
        EbeanTestUtils.createTestServer(EbeanTimelineServiceTest.class.getSimpleName());
    _aspectDao = new EbeanAspectDao(server);
    _aspectDao.setConnectionValidated(true);
    _entityTimelineService = new TimelineServiceImpl(_aspectDao, _testEntityRegistry);
    _mockProducer = mock(EventProducer.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    _entityServiceImpl =
        new EntityServiceImpl(
            _aspectDao,
            _mockProducer,
            _testEntityRegistry,
            true,
            _mockUpdateIndicesService,
            preProcessHooks);
  }

  /**
   * Ideally, all tests would be in the base class, so they're reused between all implementations.
   * When that's the case - test runner will ignore this class (and its base!) so we keep this dummy
   * test to make sure this class will always be discovered.
   */
  @Test
  public void obligatoryTest() throws Exception {
    Assert.assertTrue(true);
  }
}
