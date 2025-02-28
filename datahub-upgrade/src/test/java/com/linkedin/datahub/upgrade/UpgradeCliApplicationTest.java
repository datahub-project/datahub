package com.linkedin.datahub.upgrade;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.dao.throttle.NoOpSensor;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import io.datahubproject.metadata.context.TraceContext;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    args = {"-u", "SystemUpdate"},
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class})
public class UpgradeCliApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("restoreIndices")
  private RestoreIndices restoreIndices;

  @Autowired
  @Named("buildIndices")
  private BlockingSystemUpgrade buildIndices;

  @Autowired private ESIndexBuilder esIndexBuilder;

  @Qualifier("kafkaThrottle")
  @Autowired
  private ThrottleSensor kafkaThrottle;

  @Autowired private TraceContext traceContext;

  @Test
  public void testRestoreIndicesInit() {
    /*
     This might seem like a simple test however it does exercise the spring autowiring of the kafka health check bean
    */
    assertTrue(restoreIndices.steps().size() >= 3);
  }

  @Test
  public void testBuildIndicesInit() {
    assertEquals("BuildIndices", buildIndices.id());
    assertTrue(buildIndices.steps().size() >= 3);
    assertNotNull(esIndexBuilder.getElasticSearchConfiguration());
    assertNotNull(esIndexBuilder.getElasticSearchConfiguration().getBuildIndices());
    assertTrue(esIndexBuilder.getElasticSearchConfiguration().getBuildIndices().isCloneIndices());
    assertFalse(
        esIndexBuilder.getElasticSearchConfiguration().getBuildIndices().isAllowDocCountMismatch());
  }

  @Test
  public void testNoThrottle() {
    assertEquals(
        new NoOpSensor(), kafkaThrottle, "No kafka throttle controls expected in datahub-upgrade");
  }

  @Test
  public void testTraceContext() {
    assertNotNull(traceContext);
  }
}
